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

import sys
from unittest import TestCase
from mongomotor import Document, connect, disconnect
from mongomotor.fields import StringField
from mongomotor.queryset import QuerySet
from tests import async_test


class QuerySetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        connect(db)

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        class TestDoc(Document):
            a = StringField()

        self.test_doc = TestDoc

    @async_test
    def tearDown(self):
        yield from self.test_doc.drop_collection()

    @async_test
    def test_to_list(self):
        for i in range(4):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)
        qs = qs.filter(a__in=['1', '2'])
        docs = yield from qs.to_list()
        self.assertEqual(len(docs), 2)
        self.assertTrue(isinstance(docs[0], self.test_doc))

    @async_test
    def test_get(self):
        d = self.test_doc(a=str(1))
        yield from d.save()
        dd = self.test_doc(a=str(2))
        yield from dd.save()
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)

        returned = yield from qs.get(id=d.id)
        self.assertEqual(d.id, returned.id)

    @async_test
    def test_get_with_no_doc(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(self.test_doc.DoesNotExist):
            yield from qs.get(a='bla')

    @async_test
    def test_get_with_multiple_docs(self):
        d = self.test_doc(a='a')
        yield from d.save()
        d = self.test_doc(a='a')
        yield from d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(self.test_doc.MultipleObjectsReturned):
            yield from qs.get(a='a')

    @async_test
    def test_delete_queryset(self):
        d = self.test_doc(a='a')
        yield from d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        yield from qs.delete()

        docs = yield from qs.to_list()
        self.assertEqual(len(docs), 0)
