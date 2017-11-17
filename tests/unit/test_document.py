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

from unittest import TestCase
from unittest.mock import patch
import mongoengine
from mongomotor import Document, disconnect
from mongomotor.fields import IntField, ListField, ReferenceField
from tests import async_test, connect2db


class DocumentTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        class TestRef(Document):
            pass

        class TestDoc(Document):
            i = IntField()
            refs_list = ListField(ReferenceField(TestRef))

        class IndexedTest(Document):
            meta = {'auto_create_index': False,
                    'indexes': [{'fields': ['some_index']}]}

            some_index = IntField(index=True)

        class AutoIndexedTest(Document):
            meta = {'indexes': [{'fields': ['some_index']}]}

            some_index = IntField(index=True)

        class UniqueGuy(Document):
            attr = IntField(unique=True)

        self.ref_doc = TestRef
        self.test_doc = TestDoc
        self.indexed_test = IndexedTest
        self.auto_indexed_test = AutoIndexedTest
        self.unique = UniqueGuy

    @async_test
    def tearDown(self):
        yield from self.ref_doc.drop_collection()
        yield from self.test_doc.drop_collection()
        yield from self.indexed_test.drop_collection()
        yield from self.auto_indexed_test.drop_collection()
        yield from self.unique.drop_collection()

    @async_test
    def test_save(self):
        doc = self.test_doc(i=1)
        self.assertFalse(doc.id)
        yield from doc.save()
        self.assertTrue(doc.id)

    @async_test
    def test_save_unique(self):
        doc = self.unique(attr=1)
        self.unique.ensure_indexes()
        yield from doc.save()
        other = self.unique(attr=1)
        with self.assertRaises(mongoengine.errors.NotUniqueError):
            yield from other.save()

    @patch('mongoengine.signals.post_delete')
    def test_delete(self, *args, **kwargs):
        doc = self.test_doc(i=1)
        yield from doc.save()

        yield from doc.delete()

        self.assertTrue(mongoengine.signals.post_delete.called)

    @async_test
    def test_update(self):
        d = self.test_doc(i=1)
        yield from d.save()
        yield from d.update(i=2)
        d = yield from self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.i, 2)

    @async_test
    def test_modify(self):
        d = self.test_doc(i=1)
        yield from d.save()
        yield from d.modify(i=2)
        d = yield from self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.i, 2)

    @async_test
    def test_modify_without_pk(self):
        d = self.test_doc(i=1)
        with self.assertRaises(mongoengine.errors.InvalidDocumentError):
            yield from d.modify(i=2)

    @async_test
    def test_modify_with_bad_pk(self):
        d = self.test_doc(i=1)
        yield from d.save()
        with self.assertRaises(mongoengine.errors.InvalidQueryError):
            yield from d.modify(i=2, _id="123")

    @async_test
    def test_compare_indexes(self):

        inst = self.indexed_test(some_index=1)
        yield from inst.save()
        missing = self.indexed_test.compare_indexes()['missing']
        self.assertEqual(missing[0][0][0], 'some_index')

    @async_test
    def test_ensure_indexes(self):

        inst = self.auto_indexed_test(some_index=1)
        self.auto_indexed_test.ensure_indexes()
        yield from inst.save()
        missing = self.auto_indexed_test.compare_indexes()['missing']
        self.assertFalse(missing)

    @async_test
    def test_reload_document(self):
        ref = self.ref_doc()
        yield from ref.save()
        d = self.test_doc(i=1, refs_list=[ref])
        yield from d.save()

        yield from self.test_doc.objects(id=d.id).update(i=2)
        yield from d.reload()
        self.assertEqual(d.i, 2)

    @async_test
    def test_reload_document_references(self):
        ref = self.ref_doc()
        yield from ref.save()
        d = self.test_doc(i=1, refs_list=[])
        yield from d.save()

        yield from self.test_doc.objects(id=d.id).update(refs_list=[ref])

        yield from d.reload()

        refs = yield from d.refs_list
        self.assertEqual(len(refs), 1)
