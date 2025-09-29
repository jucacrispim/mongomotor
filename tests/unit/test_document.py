# -*- coding: utf-8 -*-

# Copyright 2016, 2025 Juca Crispim <juca@poraodojuca.net>

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
from unittest.mock import patch, Mock
import mongoengine
from mongomotor import Document, disconnect
from mongomotor import document
from mongomotor.fields import IntField, ListField, ReferenceField
from tests import async_test, connect2db


class DocumentTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db()

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
    async def tearDown(self):
        await self.ref_doc.drop_collection()
        await self.test_doc.drop_collection()
        await self.indexed_test.drop_collection()
        await self.auto_indexed_test.drop_collection()
        await self.unique.drop_collection()

    @async_test
    async def test_save(self):
        doc = self.test_doc(i=1)
        self.assertFalse(doc.id)
        await doc.save()
        self.assertTrue(doc.id)

    @async_test
    async def test_save_update(self):
        doc = self.test_doc(i=1)
        self.assertFalse(doc.id)
        await doc.save()
        doc.i = 2
        await doc.save()
        self.assertTrue(doc.id)

    @async_test
    async def test_save_unique(self):
        doc = self.unique(attr=1)
        await self.unique.ensure_indexes()
        await doc.save()
        other = self.unique(attr=1)
        with self.assertRaises(mongoengine.errors.NotUniqueError):
            await other.save()

    @patch.object(document, 'signals', Mock())
    @async_test
    async def test_delete(self, *args, **kwargs):
        doc = self.test_doc(i=1)
        await doc.save()
        await doc.delete()

        self.assertTrue(document.signals.post_delete.send.called)

    @patch.object(document, 'signals', Mock())
    @async_test
    async def test_delete_subclass(self, *args, **kwargs):

        class extended(Document):

            async def delete(self):
                return await super().delete()

        doc = extended()
        await doc.save()
        await doc.delete()

        self.assertTrue(document.signals.post_delete.send.called)

    @async_test
    async def test_update(self):
        d = self.test_doc(i=1)
        await d.save()
        await d.update(i=2)
        d = await self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.i, 2)

    @async_test
    async def test_modify(self):
        d = self.test_doc(i=1)
        await d.save()
        await d.modify(i=2)
        d = await self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.i, 2)

    @async_test
    async def test_modify_without_pk(self):
        d = self.test_doc(i=1)
        with self.assertRaises(mongoengine.errors.InvalidDocumentError):
            await d.modify(i=2)

    @async_test
    async def test_modify_with_bad_pk(self):
        d = self.test_doc(i=1)
        await d.save()
        with self.assertRaises(mongoengine.errors.InvalidQueryError):
            await d.modify(i=2, _id="123")

    @async_test
    async def test_compare_indexes(self):

        inst = self.indexed_test(some_index=1)
        await inst.save()
        missing = (await self.indexed_test.compare_indexes())['missing']
        self.assertEqual(missing[0][0][0], 'some_index')

    @async_test
    async def test_ensure_indexes(self):

        inst = self.auto_indexed_test(some_index=1)
        await self.auto_indexed_test.ensure_indexes()
        await inst.save()
        missing = (await self.auto_indexed_test.compare_indexes())['missing']
        self.assertFalse(missing)

    @async_test
    async def test_reload_document(self):
        ref = self.ref_doc()
        await ref.save()
        d = self.test_doc(i=1, refs_list=[ref])
        await d.save()

        await self.test_doc.objects(id=d.id).update(i=2)
        await d.reload()
        self.assertEqual(d.i, 2)

    @async_test
    async def test_reload_document_references(self):
        ref = self.ref_doc()
        await ref.save()
        d = self.test_doc(i=1, refs_list=[])
        await d.save()

        await self.test_doc.objects(id=d.id).update(refs_list=[ref])

        await d.reload()
        refs = await d.refs_list
        self.assertEqual(len(refs), 1)
