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


from bson.objectid import ObjectId
import os
import tornado
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from mongoengine.errors import OperationError
from mongomotor import disconnect
from mongomotor import Document, EmbeddedDocument
from mongomotor.fields import (StringField, IntField, ListField, DictField,
                               EmbeddedDocumentField, ReferenceField,
                               FileField, GenericReferenceField)
from tests import connect2db
from tests.functional import DATA_DIR, CANNOT_EXEC_JS


class MongoMotorTest(AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        connect2db(async_framework='tornado')

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

    @gen_test
    def tearDown(self):
        yield self.maindoc.drop_collection()
        yield self.refdoc.drop_collection()
        yield self.otherdoc.drop_collection()
        yield self.refdoc.drop_collection()

        super(MongoMotorTest, self).tearDown()

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    @gen_test
    def test_create(self):
        """Ensure that a new document can be added into the database
        """
        embedref = self.embedref(list_field=['uma', 'lista', 'nota', 10])
        ref = yield self.refdoc.objects.create(
            refname='refname', embedlist=[embedref])

        # asserting if our reference document was created
        self.assertTrue(ref.id)
        # and if the listfield is ok
        embedlist = ref.embedlist
        self.assertEqual(embedlist[0].list_field,
                         ['uma', 'lista', 'nota', 10])

        # creating the main document
        embed = self.embed(dict_field={'key': 'value'})
        main = self.maindoc(docname='docname', docint=1)
        main.list_field = ['list', 'of', 'strings']
        main.embedded = embed
        main.ref = ref
        yield main.save()

        # asserting if our main document was created
        self.assertTrue(main.id)
        # and if the reference points to the right place.
        # note that you need to yield reference fields.
        self.assertEqual((yield main.ref), ref)

    @gen_test
    def test_save_with_no_ref(self):
        """Ensure that a document which has a ReferenceField can
        be saved with the referece being None.
        """
        # remebering from a wired bug
        doc = self.maindoc()
        yield doc.save()
        self.assertIsNone((yield doc.ref))

    @gen_test
    def test_update_queryset(self):
        docs = [self.maindoc(docint=1) for i in range(3)]
        yield self.maindoc.objects.insert(docs)
        yield self.maindoc.objects(docint=1).update(docint=2)
        count = yield self.maindoc.objects(docint=2).count()
        self.assertEqual(count, 3)

    @gen_test
    def test_update_one_queryset(self):
        docs = [self.maindoc(docint=1) for i in range(3)]
        yield self.maindoc.objects.insert(docs)
        yield self.maindoc.objects(docint=1).update_one(docint=2)
        count = yield self.maindoc.objects(docint=2).count()
        self.assertEqual(count, 1)

    @gen_test
    def test_get_reference_after_get(self):
        """Ensures that a reference field is dereferenced properly after
        retrieving a object from database."""
        d1 = self.maindoc()
        yield d1.save()
        doc = yield self.maindoc.objects.get(id=d1.id)
        self.assertIsNone((yield doc.ref))

    @gen_test
    def test_get_real_reference(self):
        """Ensures that a reference field point to something works."""

        r = self.refdoc(refname='r')
        yield r.save()
        d = self.maindoc(docname='d', ref=r)
        yield d.save()

        d = yield self.maindoc.objects.get(id=d.id)

        self.assertTrue((yield d.ref).id)

    @gen_test
    def test_get_reference_from_class(self):
        """Ensures that getting a reference from a class does not returns
        a future"""

        ref = getattr(self.maindoc, 'ref')
        self.assertTrue(isinstance(ref, ReferenceField), ref)

    @gen_test
    def test_delete(self):
        """Ensure that a document can be deleted from the database
        """
        to_delete = self.maindoc(docname='delete!')

        yield to_delete.save()

        # asserting if the document was created
        self.assertTrue(to_delete.id)
        delid = to_delete.id

        yield to_delete.delete()

        # now making sure the document was deleted
        with self.assertRaises(self.maindoc.DoesNotExist):
            yield self.maindoc.objects.get(id=delid)

    @gen_test
    def test_query_count(self):
        """Ensure that we can count the results of a query using count()
        """
        yield self._create_data()

        # note here that we need to yield to count()
        count = yield self.maindoc.objects.count()
        self.assertEqual(count, 3)

        with self.assertRaises(Exception):
            # len does not work with mongomotor
            len(self.maindoc.objects.count())

    @gen_test
    def test_query_get(self):
        """Ensure that we can retrieve a document from database with get()
        """
        yield self._create_data()

        # Note here that we have to use yield with get()
        d = yield self.maindoc.objects.get(docname='d1')
        self.assertTrue(d.id)

    @gen_test
    def test_query_filter(self):
        """Ensure that a queryset can be filtered
        """
        yield self._create_data()

        # finding all documents without a reference
        objs = self.maindoc.objects.filter(ref=None)
        # make sure we got the proper query
        count = yield objs.count()
        self.assertEqual(count, 1)

        # now finding all documents with reference
        objs = self.maindoc.objects.filter(ref__ne=None)
        # iterating over it and checking if the documents are ok.
        # note that we don't use for loops, but iterate in the
        # motor style using fetch_next/next_object instead.
        while (yield objs.fetch_next):
            obj = objs.next_object()
            self.assertTrue(obj.id)

        self.assertEqual((yield objs.count()), 2)

    @gen_test
    def test_query_order_by(self):
        """Ensure that a queryset can be ordered using order_by()
        """
        yield self._create_data()

        objs = self.maindoc.objects.order_by('docint')
        obj = yield objs[0]
        self.assertEqual(obj.docint, 0)

        objs = self.maindoc.objects.order_by('-docint')
        obj = yield objs[0]
        self.assertEqual(obj.docint, 2)

    @gen_test
    def test_map_reduce(self):
        d = self.maindoc(list_field=['a', 'b'])
        yield d.save()
        d = self.maindoc(list_field=['a', 'c'])
        yield d.save()

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
        r = yield self.maindoc.objects.all().map_reduce(mapf, reducef,
                                                        {'merge': 'testcol'})
        self.assertEqual(r['result'], 'testcol')

    @gen_test
    def test_query_item_frequencies(self):
        """Ensure that item_frequencies method works properly
        """
        yield self._create_data()

        freq = yield self.maindoc.objects.item_frequencies('list_field')
        self.assertEqual(freq['string0'], 3)

    @gen_test
    def test_query_to_list(self):
        """Ensure that a list can be made from a queryset using to_list()
        """
        yield self._create_data()

        # note here that again we need to yield something.
        # In this case, we use yield with to_list()

        lista = yield self.maindoc.objects.to_list()
        self.assertEqual(len(lista), 3)

    @gen_test
    def test_query_to_list_with_empty_queryset(self):
        """Ensure that a list can be made from a queryset using to_list() when
        the queryset is empty
        """

        yield self.maindoc.objects.delete()
        lista = yield self.maindoc.objects.to_list()
        self.assertEqual(len(lista), 0)

    @gen_test
    def test_query_to_list_with_in_operator(self):
        yield self._create_data()
        mydict = {'d0': True, 'd1': True}
        mylist = yield self.maindoc.objects.filter(
            docname__in=mydict.keys()).to_list()

        self.assertTrue(len(mylist), 2)

    @gen_test
    def test_query_average(self):
        """Ensure that we can get the average of a field using average()
        """
        yield self._create_data()

        avg = yield self.maindoc.objects.average('docint')
        self.assertEqual(avg, 1)

    @gen_test
    def test_query_aggregate_average(self):
        """Ensure we can get the average of a field using aggregate_average()
        """
        yield self._create_data()

        avg = yield self.maindoc.objects.aggregate_average('docint')
        self.assertEqual(avg, 1)

    @gen_test
    def test_query_sum(self):
        """Ensure that we can get the sum of a field using sum()
        """
        yield self._create_data()

        summed = yield self.maindoc.objects.sum('docint')
        self.assertEqual(summed, 3)

    @gen_test
    def test_query_aggregate_sum(self):
        """Ensure that we can get the sum of a field using aggregate_sum()
        """
        yield self._create_data()

        summed = yield self.maindoc.objects.aggregate_sum('docint')
        self.assertEqual(summed, 3)

    @gen_test
    def test_distinct(self):
        """ Ensure distinct method works properly
        """
        d1 = self.maindoc(docname='d1')
        yield d1.save()
        d2 = self.maindoc(docname='d2')
        yield d2.save()

        expected = ['d1', 'd2']

        returned = yield self.maindoc.objects.distinct('docname')
        self.assertEqual(expected, returned)

    @gen_test
    def test_first(self):
        """ Ensure that first() method works properly
        """

        d1 = self.maindoc(docname='d1')
        yield d1.save()
        d2 = self.maindoc(docname='d2')
        yield d2.save()

        returned = yield self.maindoc.objects.order_by('docname').first()
        self.assertEqual(d1, returned)

    @gen_test
    def test_first_with_empty_queryset(self):
        returned = yield self.maindoc.objects.order_by('docname').first()
        self.assertFalse(returned)

    @gen_test
    def test_first_with_slice(self):
        d1 = self.maindoc(docname='d1')
        yield d1.save()
        d2 = self.maindoc(docname='d2')
        yield d2.save()

        queryset = self.maindoc.objects.order_by('docname')[1:2]
        returned = yield queryset.first()
        queryset = self.maindoc.objects.order_by('docname').skip(1)
        returned = yield queryset.first()

        self.assertEqual(d2, returned)

    @gen_test
    def test_document_dereference_with_list(self):
        r = self.refdoc()
        yield r.save()

        m = self.maindoc(reflist=[r])
        yield m.save()

        m = yield self.maindoc.objects.all()[0]

        reflist = yield m.reflist
        self.assertEqual(len(reflist), 1)

        m = yield self.maindoc.objects.get(id=m.id)

        reflist = yield getattr(m, 'reflist')
        self.assertEqual(len(reflist), 1)

        mlist = yield self.maindoc.objects.all().to_list()
        for m in mlist:
            reflist = yield getattr(m, 'reflist')
            self.assertEqual(len(reflist), 1)

        mlist = self.maindoc.objects.all()
        while (yield mlist.fetch_next):
            m = mlist.next_object()
            reflist = yield getattr(m, 'reflist')
            self.assertEqual(len(reflist), 1)

    @gen_test
    def test_complex_base_field_get(self):
        r = self.refdoc()
        yield r.save()

        m = self.maindoc(reflist=[r])
        yield m.save()

        # when it is a reference it is a future
        self.assertEqual(len((yield m.reflist)), 1)

        m = yield self.maindoc.objects.get(id=m.id)
        self.assertEqual(len((yield m.reflist)), 1)

        # no ref, no future
        m = self.maindoc(list_field=['a', 'b'])
        yield m.save()

        m = yield self.maindoc.objects.get(id=m.id)

        self.assertEqual(m.list_field, ['a', 'b'])

    @gen_test
    def test_complex_base_field_get_with_empty_object(self):
        m = self.maindoc(reflist=[])
        yield m.save()
        m = yield self.maindoc.objects.get(id=m.id)
        self.assertIsInstance(m.reflist, tornado.concurrent.Future)
        reflist = yield m.reflist
        self.assertFalse(reflist)

    @gen_test
    def test_query_skip(self):
        """ Ensure that the skip method works properly. """
        m0 = self.maindoc(docname='dz')
        m1 = self.maindoc(docname='dx')
        yield m0.save()
        yield m1.save()

        d = self.maindoc.objects.order_by('-docname').skip(1)
        d = yield d[0]
        self.assertEqual(d, m1)

    @gen_test
    def test_delete_query_skip_without_documents(self):
        """Ensures that deleting a empty queryset works."""

        to_delete = self.maindoc.objects.skip(10)
        yield to_delete.delete()
        count = yield self.maindoc.objects.skip(10).count()
        self.assertEqual(count, 0)

    @gen_test
    def test_update_document(self):
        """Ensures that updating a document works properly."""

        doc = self.maindoc(docname='d0')
        yield doc.save()

        yield doc.update(set__docname='d1')
        yield gen.sleep(0.1)
        doc = yield self.maindoc.objects.get(docname='d1')

        self.assertTrue(doc.id)

    @gen_test
    def test_bulk_insert(self):
        docs = [self.maindoc(docname='d{}'.format(i)) for i in range(3)]
        ret = yield self.maindoc.objects.insert(docs)
        self.assertEqual(len(ret), 3)

    @gen_test
    def test_insert_document_with_operation_error(self):
        """Ensures that inserting a doc already saved raises."""

        doc = self.maindoc(docname='d0')
        yield doc.save()

        with self.assertRaises(OperationError):
            doc = yield self.maindoc.objects.insert([doc])

    @gen_test
    def test_aggregate(self):
        d = self.maindoc(list_field=['a', 'b'])
        yield d.save()
        d = self.maindoc(list_field=['a', 'c'])
        yield d.save()

        group = {'$group': {'_id': '$list_field',
                            'total': {'$sum': 1}}}
        unwind = {'$unwind': '$list_field'}

        cursor = self.maindoc.objects.aggregate(unwind, group)

        while (yield cursor.fetch_next):
            d = cursor.next_object()
            if d['_id'] == 'a':
                self.assertEqual(d['total'], 2)
            else:
                self.assertEqual(d['total'], 1)

    @gen_test
    def test_modify_upsert(self):
        """Ensures that queryset modify works upserting."""

        r = yield self.maindoc.objects.modify(upsert=True, new=True,
                                              docname='doc')
        self.assertTrue(r.id)

    @gen_test
    def test_modify(self):
        """Ensures that queryset modify works."""
        d = self.maindoc(docname='dn')
        yield d.save()
        r = yield self.maindoc.objects.modify(new=True,  id=d.id,
                                              docname='dnn')
        self.assertEqual(r.docname, 'dnn')

    @gen_test
    def test_modify_unknown_object(self):
        yield self.maindoc.objects.modify(id=ObjectId(), docname='dn')
        total = yield self.maindoc.objects.all().count()

        self.assertEqual(total, 0)
        self.assertFalse(None)

    if not CANNOT_EXEC_JS:
        @gen_test
        def test_exec_js(self):
            d = self.maindoc(list_field=['a', 'b'])
            yield d.save()
            r = yield self.maindoc.objects.exec_js('db.getCollectionNames()')
            self.assertTrue(r)

    @gen_test
    def test_generic_reference(self):
        r = self.refdoc()
        yield r.save()
        d = self.genericdoc(some_field='asdf', ref=r)
        yield d.save()
        yield d.reload()
        ref = yield d.ref
        self.assertEqual(r, ref)

    @gen.coroutine
    def _create_data(self):
        # here we create the following data:
        # 3 instances of MainDocument, naming d0, d1 and d2.
        # 2 of these instances have references, one has not.
        r = self.refdoc()
        yield r.save()
        to_list_field = ['string0', 'string1', 'string2']
        for i in range(3):
            d = self.maindoc(docname='d%s' % i)
            d.docint = i
            d.list_field = to_list_field[:i + 1]
            if i < 2:
                d.ref = r

            yield d.save()


class GridFSTest(AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        connect2db(async_framework='tornado')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            filefield = FileField()

        self.test_doc = TestDoc

    @gen_test
    def tearDown(self):
        yield self.test_doc.drop_collection()
        yield self.test_doc._get_db().fs.files.remove()
        yield self.test_doc._get_db().fs.chunks.remove()

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    @gen_test
    def test_put_file(self):
        filepath = os.path.join(DATA_DIR, 'file.txt')
        doc = self.test_doc()
        fd = open(filepath, 'rb')
        fcontents = fd.read()
        fd.close()

        yield doc.filefield.put(fcontents, mime_type='plain/text')
        yield doc.save()
        doc = yield self.test_doc.objects.get(id=doc.id)
        self.assertEqual((yield doc.filefield.read()), fcontents)
        self.assertEqual(doc.filefield.mime_type, 'plain/text')
