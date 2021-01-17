# -*- coding: utf-8 -*-

# Copyright 2016 Juca Crispim <juca@poraodojuca.net>

# This file is part of mongomotor.

# mongomotor is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# mongomotor is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with mongomotor. If not, see <http://www.gnu.org/licenses/>.


import asyncio
from bson.objectid import ObjectId
import os
import unittest
from mongoengine.errors import OperationError
from mongomotor import disconnect
from mongomotor import Document, EmbeddedDocument
from mongomotor.fields import (StringField, IntField, ListField, DictField,
                               EmbeddedDocumentField, ReferenceField,
                               FileField, GenericReferenceField)

from tests import async_test, connect2db
from tests.functional import DATA_DIR, CANNOT_EXEC_JS


class MongoMotorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        super(MongoMotorTest, self).setUp()

        # some models to simple tests over
        # mongomotor

        class EmbedRef(EmbeddedDocument):
            list_field = ListField()

        class RefDoc(Document):
            refname = StringField()
            embedlist = ListField(EmbeddedDocumentField(EmbedRef))

        class SuperDoc(Document):
            some_field = StringField()
            reflist = ListField(ReferenceField(RefDoc))

            meta = {'allow_inheritance': True}

        class OtherDoc(SuperDoc):
            pass

        class Embed(EmbeddedDocument):
            dict_field = DictField()
            list_field = ListField()
            ref = ReferenceField(RefDoc)

        class MainDoc(SuperDoc):
            docname = StringField()
            docint = IntField()
            list_field = ListField(StringField())
            embedded = EmbeddedDocumentField(Embed)
            ref = ReferenceField(RefDoc)

        class GenericRefDoc(SuperDoc):
            ref = GenericReferenceField()

        self.maindoc = MainDoc
        self.embed = Embed
        self.refdoc = RefDoc
        self.embedref = EmbedRef
        self.otherdoc = OtherDoc
        self.genericdoc = GenericRefDoc

    @async_test
    async def tearDown(self):
        await self.maindoc.drop_collection()
        await self.refdoc.drop_collection()
        await self.otherdoc.drop_collection()
        await self.refdoc.drop_collection()

        super(MongoMotorTest, self).tearDown()

    @async_test
    async def test_create(self):
        """Ensure that a new document can be added into the database
        """
        embedref = self.embedref(list_field=['uma', 'lista', 'nota', 10])
        ref = await self.refdoc.objects.create(
            refname='refname', embedlist=[embedref])

        # asserting if our reference document was created
        self.assertTrue(ref.id)
        # and if the listfield is ok
        embedlist = ref.embedlist
        self.assertEqual(embedlist[0].list_field, ['uma', 'lista', 'nota', 10])

        # creating the main document
        embed = self.embed(dict_field={'key': 'value'})
        main = self.maindoc(docname='docname', docint=1)
        main.list_field = ['list', 'of', 'strings']
        main.embedded = embed
        main.ref = ref
        await main.save()

        # asserting if our main document was created
        self.assertTrue(main.id)
        # and if the reference points to the right place.
        # note that you need to yield reference fields.
        self.assertEqual((await main.ref), ref)

    @async_test
    async def test_save_with_no_ref(self):
        """Ensure that a document which has a ReferenceField can
        be saved with the referece being None.
        """
        # remebering from a wired bug
        # the thing is: on document constructor mongoengine tries to
        # set default values and that makes an None reference became
        # a future.
        doc = self.maindoc()
        await doc.save()
        self.assertIsNone((await doc.ref))

    @async_test
    async def test_update_queryset(self):
        docs = [self.maindoc(docint=1) for i in range(3)]
        await self.maindoc.objects.insert(docs)
        await self.maindoc.objects(docint=1).update(docint=2)
        count = await self.maindoc.objects(docint=2).count()
        self.assertEqual(count, 3)

    @async_test
    async def test_update_one_queryset(self):
        docs = [self.maindoc(docint=1) for i in range(3)]
        await self.maindoc.objects.insert(docs)
        await self.maindoc.objects(docint=1).update_one(docint=2)
        count = await self.maindoc.objects(docint=2).count()
        self.assertEqual(count, 1)

    @async_test
    async def test_get_reference_after_get(self):
        """Ensures that a reference field is dereferenced properly after
        retrieving a object from database."""
        d1 = self.maindoc()
        await d1.save()
        doc = await self.maindoc.objects.get(id=d1.id)
        self.assertIsNone((await doc.ref))

    @async_test
    async def test_get_real_reference(self):
        """Ensures that a reference field point to something works."""

        r = self.refdoc(refname='r')
        await r.save()
        d = self.maindoc(docname='d', ref=r)
        await d.save()

        d = await self.maindoc.objects.get(id=d.id)

        self.assertTrue((await d.ref).id)

    @async_test
    async def test_get_reference_from_class(self):
        """Ensures that getting a reference from a class does not returns
        a future"""

        ref = getattr(self.maindoc, 'ref')
        self.assertTrue(isinstance(ref, ReferenceField), ref)

    @async_test
    async def test_delete(self):
        """Ensure that a document can be deleted from the database
        """
        to_delete = self.maindoc(docname='delete!')

        await to_delete.save()

        # asserting if the document was created
        self.assertTrue(to_delete.id)
        delid = to_delete.id

        await to_delete.delete()

        # now making sure the document was deleted
        with self.assertRaises(self.maindoc.DoesNotExist):
            await self.maindoc.objects.get(id=delid)

    @async_test
    async def test_query_count(self):
        """Ensure that we can count the results of a query using count()
        """
        await self._create_data()

        # note here that we need to yield to count()

        count = await self.maindoc.objects.count()
        self.assertEqual(count, 3)

        with self.assertRaises(Exception):
            # len does not work with mongomotor
            len(self.maindoc.objects.count())

    @async_test
    async def test_query_get(self):
        """Ensure that we can retrieve a document from database with get()
        """
        await self._create_data()

        # Note here that we have to use yield with get()
        d = await self.maindoc.objects.get(docname='d1')
        self.assertTrue(d.id)

    @async_test
    async def test_query_filter(self):
        """Ensure that a queryset can be filtered
        """
        await self._create_data()

        # finding all documents without a reference
        objs = self.maindoc.objects.filter(ref=None)
        # make sure we got the proper query
        count = await objs.count()
        self.assertEqual(count, 1)

        # now finding all documents with reference
        objs = self.maindoc.objects.filter(ref__ne=None)
        async for obj in objs:
            self.assertTrue(obj.id)

        self.assertEqual((await objs.count()), 2)

    @async_test
    async def test_query_order_by(self):
        """Ensure that a queryset can be ordered using order_by()
        """
        await self._create_data()

        objs = self.maindoc.objects.order_by('docint')
        obj = await objs[0]
        self.assertEqual(obj.docint, 0)

        objs = self.maindoc.objects.order_by('-docint')
        obj = await objs[0]
        self.assertEqual(obj.docint, 2)

    @async_test
    async def test_map_reduce(self):
        d = self.maindoc(list_field=['a', 'b'])
        await d.save()
        d = self.maindoc(list_field=['a', 'c'])
        await d.save()

        mapf = """
function(){
  this.list_field.forEach(function(f){
    emit(f, 1);
  });
}
"""
        reducef = """
function(key, values){
  return Array.sum(values)
}
"""
        r = await self.maindoc.objects.all().map_reduce(
            mapf, reducef, {'merge': 'testcol'})
        self.assertEqual(r['result'], 'testcol')

    @async_test
    async def test_query_item_frequencies(self):
        """Ensure that item_frequencies method works properly
        """
        await self._create_data()

        freq = await self.maindoc.objects.item_frequencies('list_field')
        self.assertEqual(freq['string0'], 3)

    @async_test
    async def test_query_to_list(self):
        """Ensure that a list can be made from a queryset using to_list()
        """
        await self._create_data()

        # note here that again we need to yield something.
        # In this case, we use yield with to_list()

        lista = await self.maindoc.objects.to_list()
        self.assertEqual(len(lista), 3)

    @async_test
    async def test_query_to_list_with_empty_queryset(self):
        """Ensure that a list can be made from a queryset using to_list() when
        the queryset is empty
        """

        await self.maindoc.objects.delete()
        lista = await self.maindoc.objects.to_list()
        self.assertEqual(len(lista), 0)

    @async_test
    async def test_query_to_list_with_in_operator(self):
        await self._create_data()
        mydict = {'d0': True, 'd1': True}
        mylist = await self.maindoc.objects.filter(
            docname__in=mydict.keys()).to_list()

        self.assertTrue(len(mylist), 2)

    @async_test
    async def test_query_average(self):
        """Ensure that we can get the average of a field using average()
        """
        await self._create_data()

        avg = await self.maindoc.objects.average('docint')
        self.assertEqual(avg, 1)

    @async_test
    async def test_query_aggregate_average(self):
        """Ensure we can get the average of a field using aggregate_average()
        """
        await self._create_data()

        avg = await self.maindoc.objects.aggregate_average('docint')
        self.assertEqual(avg, 1)

    @async_test
    async def test_query_sum(self):
        """Ensure that we can get the sum of a field using sum()
        """
        await self._create_data()

        summed = await self.maindoc.objects.sum('docint')
        self.assertEqual(summed, 3)

    @async_test
    async def test_query_aggregate_sum(self):
        """Ensure that we can get the sum of a field using aggregate_sum()
        """
        await self._create_data()

        summed = await self.maindoc.objects.aggregate_sum('docint')
        self.assertEqual(summed, 3)

    @async_test
    async def test_distinct(self):
        """ Ensure distinct method works properly
        """
        d1 = self.maindoc(docname='d1')
        await d1.save()
        d2 = self.maindoc(docname='d2')
        await d2.save()

        expected = ['d1', 'd2']

        returned = await self.maindoc.objects.distinct('docname')
        self.assertEqual(expected, returned)

    @async_test
    async def test_first(self):
        """ Ensure that first() method works properly
        """

        d1 = self.maindoc(docname='d1')
        await d1.save()
        d2 = self.maindoc(docname='d2')
        await d2.save()

        returned = await self.maindoc.objects.order_by('docname').first()
        self.assertEqual(d1, returned)

    @async_test
    async def test_first_with_empty_queryset(self):
        returned = await self.maindoc.objects.order_by('docname').first()
        self.assertFalse(returned)

    @async_test
    async def test_first_with_slice(self):
        d1 = self.maindoc(docname='d1')
        await d1.save()
        d2 = self.maindoc(docname='d2')
        await d2.save()

        queryset = self.maindoc.objects.order_by('docname')[1:2]
        returned = await queryset.first()
        queryset = self.maindoc.objects.order_by('docname').skip(1)
        returned = await queryset.first()

        self.assertEqual(d2, returned)

    @async_test
    async def test_document_dereference_with_list(self):
        r = self.refdoc()
        await r.save()

        m = self.maindoc(reflist=[r])
        await m.save()

        m = await self.maindoc.objects.all()[0]

        reflist = await m.reflist
        self.assertEqual(len(reflist), 1)

        m = await self.maindoc.objects.get(id=m.id)

        reflist = await getattr(m, 'reflist')
        self.assertEqual(len(reflist), 1)

        mlist = await self.maindoc.objects.all().to_list()
        for m in mlist:
            reflist = await getattr(m, 'reflist')
            self.assertEqual(len(reflist), 1)

        mlist = self.maindoc.objects.all()
        async for m in mlist:
            reflist = await getattr(m, 'reflist')
            self.assertEqual(len(reflist), 1)

    @async_test
    async def test_complex_base_field_get(self):
        r = self.refdoc()
        await r.save()

        m = self.maindoc(reflist=[r])
        await m.save()

        # when it is a reference it is a future
        self.assertEqual(len((await m.reflist)), 1)

        m = await self.maindoc.objects.get(id=m.id)
        self.assertEqual(len((await m.reflist)), 1)

        # no ref, no future
        m = self.maindoc(list_field=['a', 'b'])
        await m.save()

        m = await self.maindoc.objects.get(id=m.id)

        self.assertEqual(m.list_field, ['a', 'b'])

    @async_test
    async def test_complex_base_field_get_with_empty_object(self):
        m = self.maindoc(reflist=[])
        await m.save()
        m = await self.maindoc.objects.get(id=m.id)
        self.assertIsInstance(m.reflist, asyncio.futures.Future)
        reflist = await m.reflist
        self.assertFalse(reflist)

    @async_test
    async def test_query_skip(self):
        """ Ensure that the skip method works properly. """
        m0 = self.maindoc(docname='dz')
        m1 = self.maindoc(docname='dx')
        await m0.save()
        await m1.save()

        d = self.maindoc.objects.order_by('-docname').skip(1)
        d = await d[0]
        self.assertEqual(d, m1)

    @async_test
    async def test_delete_query_skip_without_documents(self):
        """Ensures that deleting a empty queryset works."""

        to_delete = self.maindoc.objects.skip(10)
        await to_delete.delete()
        count = await self.maindoc.objects.skip(10).count()
        self.assertEqual(count, 0)

    @async_test
    async def test_update_document(self):
        """Ensures that updating a document works properly."""

        doc = self.maindoc(docname='d0')
        await doc.save()

        await doc.update(set__docname='d1')

        doc = await self.maindoc.objects.get(docname='d1')

        self.assertTrue(doc.id)

    @async_test
    async def test_bulk_insert(self):
        docs = [self.maindoc(docname='d{}'.format(i)) for i in range(3)]
        ret = await self.maindoc.objects.insert(docs)
        self.assertEqual(len(ret), 3)

    @async_test
    async def test_insert_document_with_operation_error(self):
        """Ensures that inserting a doc already saved raises."""

        doc = self.maindoc(docname='d0')
        await doc.save()

        with self.assertRaises(OperationError):
            doc = await self.maindoc.objects.insert([doc])

    @async_test
    async def test_aggregate(self):
        d = self.maindoc(list_field=['a', 'b'])
        await d.save()
        d = self.maindoc(list_field=['a', 'c'])
        await d.save()

        group = {'$group': {'_id': '$list_field',
                            'total': {'$sum': 1}}}
        unwind = {'$unwind': '$list_field'}

        cursor = self.maindoc.objects.aggregate([unwind, group])

        async for d in cursor:
            if d['_id'] == 'a':
                self.assertEqual(d['total'], 2)
            else:
                self.assertEqual(d['total'], 1)

    @async_test
    async def test_modify_upsert(self):
        """Ensures that queryset modify works upserting."""

        r = await self.maindoc.objects.modify(upsert=True, new=True,
                                              docname='doc')
        self.assertTrue(r.id)

    @async_test
    async def test_modify(self):
        """Ensures that queryset modify works."""
        d = self.maindoc(docname='dn')
        await d.save()
        r = await self.maindoc.objects.modify(new=True,  id=d.id,
                                              docname='dnn')
        self.assertEqual(r.docname, 'dnn')

    @async_test
    async def test_modify_unknown_object(self):
        await self.maindoc.objects.modify(id=ObjectId(), docname='dn')
        total = await self.maindoc.objects.all().count()

        self.assertEqual(total, 0)
        self.assertFalse(None)

    @async_test
    async def test_generic_reference(self):
        r = self.refdoc()
        await r.save()
        d = self.genericdoc(some_field='asdf', ref=r)
        await d.save()
        await d.reload()
        ref = await d.ref
        self.assertEqual(r, ref)

    @async_test
    async def test_update_doc_list_field_pull(self):
        d = self.maindoc(docint=1, list_field=['a', 'b'])
        await d.save()
        await d.update(pull__list_field='a')
        await d.reload()
        self.assertEqual(len(d.list_field), 1)

    async def _create_data(self):
        # here we created the following data:
        # 3 instances of MainDocument, naming d0, d1 and d2.
        # 2 of these instances have references, one has not.
        r = self.refdoc()
        await r.save()
        to_list_field = ['string0', 'string1', 'string2']
        futures = []
        for i in range(3):
            d = self.maindoc(docname='d%s' % i)
            d.docint = i
            d.list_field = to_list_field[:i + 1]
            if i < 2:
                d.ref = r

            f = d.save()
            futures.append(f)

        await asyncio.gather(*futures)


class GridFSTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            filefield = FileField()

        self.test_doc = TestDoc

    @async_test
    async def tearDown(self):
        await self.test_doc.drop_collection()

    @async_test
    async def test_put_file(self):
        filepath = os.path.join(DATA_DIR, 'file.txt')
        doc = self.test_doc()
        fd = open(filepath, 'rb')
        fcontents = fd.read()
        fd.close()
        await doc.filefield.put(fcontents, mime_type='plain/text')
        await doc.save()
        doc = await self.test_doc.objects.get(id=doc.id)
        self.assertEqual((await doc.filefield.read()), fcontents)
        self.assertEqual(doc.filefield.grid_out.metadata['mime_type'],
                         'plain/text')
