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
import unittest
from bson.objectid import ObjectId
import sys
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from mongoengine.errors import OperationError
from mongomotor import connect, disconnect
from mongomotor import Document, EmbeddedDocument, MapReduceDocument
from mongomotor.fields import (StringField, IntField, ListField, DictField,
                               EmbeddedDocumentField, ReferenceField)

from tests import async_test

db = 'mongomotor-test-{}{}'.format(sys.version_info.major,
                                   sys.version_info.minor)


class MongoMotorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        connect(db, async_framework='asyncio')

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

        self.maindoc = MainDoc
        self.embed = Embed
        self.refdoc = RefDoc
        self.embedref = EmbedRef
        self.otherdoc = OtherDoc

    @async_test
    def tearDown(self):
        yield from self.maindoc.drop_collection()
        yield from self.refdoc.drop_collection()
        yield from self.otherdoc.drop_collection()
        yield from self.refdoc.drop_collection()

        super(MongoMotorTest, self).tearDown()

    @async_test
    def test_create(self):
        """Ensure that a new document can be added into the database
        """
        embedref = self.embedref(list_field=['uma', 'lista', 'nota', 10])
        ref = self.refdoc(refname='refname', embedlist=[embedref])
        yield from ref.save()

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
        yield from main.save()

        # asserting if our main document was created
        self.assertTrue(main.id)
        # and if the reference points to the right place.
        # note that you need to yield reference fields.
        self.assertEqual((yield from main.ref), ref)

    @async_test
    def test_save_with_no_ref(self):
        """Ensure that a document which has a ReferenceField can
        be saved with the referece being None.
        """
        # remebering from a wired bug
        # the thing is: on document constructor mongoengine tries to
        # set default values and that makes an None reference became
        # a future.
        doc = self.maindoc()
        yield from doc.save()
        self.assertIsNone((yield from doc.ref))

    @async_test
    def test_get_reference_after_get(self):
        """Ensures that a reference field is dereferenced properly after
        retrieving a object from database."""
        d1 = self.maindoc()
        yield from d1.save()
        doc = yield from self.maindoc.objects.get(id=d1.id)
        self.assertIsNone((yield from doc.ref))

    @async_test
    def test_get_real_reference(self):
        """Ensures that a reference field point to something works."""

        r = self.refdoc(refname='r')
        yield from r.save()
        d = self.maindoc(docname='d', ref=r)
        yield from d.save()

        d = yield from self.maindoc.objects.get(id=d.id)

        self.assertTrue((yield from d.ref).id)

    @async_test
    def test_get_reference_from_class(self):
        """Ensures that getting a reference from a class does not returns
        a future"""

        ref = getattr(self.maindoc, 'ref')
        self.assertTrue(isinstance(ref, ReferenceField), ref)

    @async_test
    def test_delete(self):
        """Ensure that a document can be deleted from the database
        """
        to_delete = self.maindoc(docname='delete!')

        yield from to_delete.save()

        # asserting if the document was created
        self.assertTrue(to_delete.id)
        delid = to_delete.id

        yield from to_delete.delete()

        # now making sure the document was deleted
        with self.assertRaises(self.maindoc.DoesNotExist):
            yield from self.maindoc.objects.get(id=delid)

    @async_test
    def test_query_count(self):
        """Ensure that we can count the results of a query using count()
        """
        yield from self._create_data()

        # note here that we need to yield to count()

        count = yield from self.maindoc.objects.count()
        self.assertEqual(count, 3)

        with self.assertRaises(Exception):
            # len does not work with mongomotor
            len(self.maindoc.objects.count())

    @async_test
    def test_query_get(self):
        """Ensure that we can retrieve a document from database with get()
        """
        yield from self._create_data()

        # Note here that we have to use yield with get()
        d = yield from self.maindoc.objects.get(docname='d1')
        self.assertTrue(d.id)

    @async_test
    def test_query_filter(self):
        """Ensure that a queryset can be filtered
        """
        yield from self._create_data()

        # finding all documents without a reference
        objs = self.maindoc.objects.filter(ref=None)
        # make sure we got the proper query
        count = yield from objs.count()
        self.assertEqual(count, 1)

        # now finding all documents with reference
        objs = self.maindoc.objects.filter(ref__ne=None)
        # iterating over it and checking if the documents are ok.
        # note that we don't use for loops, but iterate in the
        # motor style using fetch_next/next_object instead.
        while (yield from objs.fetch_next):
            obj = objs.next_object()
            self.assertTrue(obj.id)

        self.assertEqual((yield from objs.count()), 2)

    @async_test
    def test_query_order_by(self):
        """Ensure that a queryset can be ordered using order_by()
        """
        yield from self._create_data()

        objs = self.maindoc.objects.order_by('docint')
        obj = yield from objs[0]
        self.assertEqual(obj.docint, 0)

        objs = self.maindoc.objects.order_by('-docint')
        obj = yield from objs[0]
        self.assertEqual(obj.docint, 2)

    @async_test
    def test_query_item_frequencies(self):
        """Ensure that item_frequencies method works properly
        """
        yield from self._create_data()

        freq = yield from self.maindoc.objects.item_frequencies('list_field')
        self.assertEqual(freq['string0'], 3)

    @asyncio.coroutine
    def _create_data(self):
        # here we create the following data:
        # 3 instances of MainDocument, naming d0, d1 and d2.
        # 2 of these instances have references, one has not.
        r = self.refdoc()
        yield from r.save()
        to_list_field = ['string0', 'string1', 'string2']
        for i in range(3):
            d = self.maindoc(docname='d%s' % i)
            d.docint = i
            d.list_field = to_list_field[:i + 1]
            if i < 2:
                d.ref = r

            yield from d.save()
