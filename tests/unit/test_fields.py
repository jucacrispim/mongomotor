# -*- coding: utf-8 -*-

# Copyright 2016-2017, 2025 Juca Crispim <juca@poraodojuca.dev>

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

import io
from unittest import TestCase
from unittest.mock import Mock
from mongoengine.connection import get_db
from motor.frameworks import asyncio as asyncio_framework
import gridfs
from mongomotor import Document, disconnect, EmbeddedDocument
from mongomotor.fields import (ReferenceField, ListField,
                               EmbeddedDocumentField, StringField, DictField,
                               BaseList, BaseDict, GridFSProxy, FileField,
                               GridFSError, GenericReferenceField)
from tests import async_test, connect2db


class TestReferenceField(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    @async_test
    async def test_get(self):
        class RefClass(Document):
            a = StringField()

        class SomeClass(Document):
            ref = ReferenceField(RefClass)

        r = RefClass(a='ola')
        await r.save()
        someclass = SomeClass(ref=r)
        await someclass.save()
        ref = await someclass.ref

        self.assertTrue(ref.id)
        self.assertEqual(ref.a, "ola")

    def test_get_with_class(self):
        class RefClass(Document):

            @classmethod
            def _get_db(self):
                db = Mock()
                db._framework = asyncio_framework
                return db

        class SomeClass(Document):
            ref = ReferenceField(RefClass)

        self.assertIsInstance(SomeClass.ref, ReferenceField)

    @async_test
    async def test_reference_field_in_a_embedded_field(self):
        try:
            class RefClass(Document):
                pass

            class Embed(EmbeddedDocument):
                ref = ReferenceField(RefClass)

            class TestDocument(Document):
                embed = EmbeddedDocumentField(Embed)

            r = RefClass()
            await r.save()
            embed = Embed(ref=r)
            d = TestDocument(embed=embed)
            await d.save()

            d = await TestDocument.objects.get(id=d.id)
            self.assertTrue((await d.embed.ref).id)
        finally:
            await RefClass.drop_collection()
            await TestDocument.drop_collection()


class GenericReferenceFieldTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    @async_test
    async def test_get(self):
        class RefClass(Document):
            pass

        class SomeClass(Document):
            ref = GenericReferenceField()

        r = RefClass()
        await r.save()
        someclass = SomeClass(ref=r)
        ref = await someclass.ref
        self.assertTrue(ref.id)

    @async_test
    async def test_get_none(self):
        class RefClass(Document):
            pass

        class SomeClass(Document):
            ref = GenericReferenceField()

        someclass = SomeClass()
        ref = await someclass.ref
        self.assertIsNone(ref)


class TestComplexField(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):

        class ReferedByEmbed(Document):
            pass

        class EmbedRef(EmbeddedDocument):
            ref = ReferenceField(ReferedByEmbed)

        class TestEmbedRef(Document):
            embedlist = ListField(EmbeddedDocumentField(EmbedRef))

        class Embed(EmbeddedDocument):
            field = StringField()

        class ReferenceClass(Document):
            embed_list = ListField(EmbeddedDocumentField(Embed))

        class TestClass(Document):
            list_field = ListField()
            dict_field = DictField()
            list_reference = ListField(ReferenceField(ReferenceClass))

        self.embed = Embed
        self.reference_class = ReferenceClass
        self.test_class = TestClass
        self.embed_ref = EmbedRef
        self.ref_by_embed = ReferedByEmbed
        self.test_embed_ref = TestEmbedRef

    @async_test
    async def tearDown(self):
        await self.reference_class.drop_collection()
        await self.test_class.drop_collection()
        await self.ref_by_embed.drop_collection()
        await self.test_embed_ref.drop_collection()

    def test_get_list_field_with_class(self):
        field = self.test_class.list_reference
        self.assertTrue(isinstance(field, ListField))

    @async_test
    async def test_get_list_field_with_string(self):
        test_doc = self.test_class(list_field=['a', 'b'])
        await test_doc.save()

        test_doc = await self.test_class.objects.get(id=test_doc.id)
        lf = test_doc.list_field
        self.assertEqual(lf, ['a', 'b'])

    @async_test
    async def test_get_list_field_with_embedded(self):
        embed = self.embed(field='bla')
        test_doc = self.reference_class(embed_list=[embed])
        await test_doc.save()

        test_doc = await self.reference_class.objects.get(id=test_doc.id)
        self.assertEqual(test_doc.embed_list, [embed])

    @async_test
    async def test_get_list_field_with_reference(self):
        ref = self.reference_class()
        await ref.save()
        test_doc = self.test_class(list_reference=[ref])
        await test_doc.save()

        test_doc = await self.test_class.objects.get(id=test_doc.id)
        refs = await test_doc.list_reference
        self.assertTrue(isinstance(refs[0], self.reference_class))

    @async_test
    async def test_get_list_field_with_empyt_references(self):
        """Ensures that a empty list of references returns a empyt list,
        not None."""
        test_doc = self.test_class()
        await test_doc.save()

        test_doc = await self.test_class.objects.get(id=test_doc.id)
        refs = await test_doc.list_reference
        self.assertIsInstance(refs, list)
        self.assertFalse(refs)

    @async_test
    async def test_convert_value_with_list(self):
        doc = self.test_class(list_field=[1, 2])
        self.assertIsInstance(doc.list_field, BaseList)

    @async_test
    async def test_convert_value_with_dict(self):
        doc = self.test_class(dict_field={'a': 1, 'b': 2})
        self.assertIsInstance(doc.dict_field, BaseDict)

    @async_test
    async def test_embedded_list_with_references(self):
        """Ensures that we can retrieve a list of embedded documents
        that has references."""

        ref = self.ref_by_embed()
        await ref.save()
        embed = self.embed_ref(ref=ref)
        doc = self.test_embed_ref(embedlist=[embed])
        await doc.save()
        doc = await self.test_embed_ref.objects.get(id=doc.id)
        ref = await doc.embedlist[0].ref
        self.assertTrue(ref.id)


class GridFSProxyTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        self.proxy = GridFSProxy()

    @async_test
    async def tearDown(self):
        db = get_db()
        coll = db.fs
        await coll.drop()
        await db.fs.files.drop()
        await db.fs.chunks.drop()

    def test_fs(self):
        self.assertIsInstance(self.proxy.fs, gridfs.AsyncGridFS)

    @async_test
    async def test_new_file(self):
        self.proxy.new_file(**{'contentType': 'text/plain'})
        self.assertTrue(self.proxy.grid_id)

    @async_test
    async def test_write_with_id_not_newfile(self):
        self.proxy.grid_id = 'some-id'
        with self.assertRaises(GridFSError):
            await self.proxy.write('some-str')

    @async_test
    async def test_write(self):
        await self.proxy.write(b'bla')
        await self.proxy.close()
        content = await self.proxy.read()
        self.assertEqual(content, b'bla')

    @async_test
    async def test_put(self):
        await self.proxy.put(b'asdf')
        await self.proxy.close()
        content = await self.proxy.read()
        self.assertEqual(content, b'asdf')

    @async_test
    async def test_read(self):
        fcontents = b'asdf'
        await self.proxy.put(fcontents)
        await self.proxy.close()
        contents = await self.proxy.read()
        self.assertEqual(fcontents, contents)

    @async_test
    async def test_replace(self):
        fcontents = b'asdf'
        new_contents = b'123'
        await self.proxy.put(fcontents)
        await self.proxy.close()
        await self.proxy.replace(new_contents)
        await self.proxy.close()
        contents = await self.proxy.read()
        self.assertEqual(contents, new_contents)


class FileFieldTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):

        class TestFileDoc(Document):
            ff = FileField()

        self.test_doc = TestFileDoc

    @async_test
    async def tearDown(self):
        await self.test_doc.drop_collection()
        db = self.test_doc._get_db()
        await db.fs.files.drop()
        await db.fs.chunks.drop()

        await db.fs.drop()

    @async_test
    async def test_file_field_put(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        await doc.ff.put(fcontents)
        self.assertTrue(doc.ff.grid_id)

    @async_test
    async def test_file_field_read(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        await doc.ff.put(fcontents)
        await doc.save()
        doc = await self.test_doc.objects.get(id=doc.id)
        contents = await doc.ff.read()
        self.assertEqual(contents, fcontents)

    def test_new_file(self):
        doc = self.test_doc()
        doc.ff.new_file()
        self.assertTrue(doc.ff.grid_id)

    @async_test
    async def test_field_write_with_already_existent_file(self):
        doc = self.test_doc()
        await doc.ff.put(b'a file')

        with self.assertRaises(GridFSError):
            await doc.ff.write('something')

    @async_test
    async def test_field_write(self):
        doc = self.test_doc()
        doc.ff.new_file()
        await doc.ff.write(b'a file')
        await doc.ff.write(b'\nthe test')
        await doc.ff.close()
        content = await doc.ff.read()
        self.assertEqual(len(content.split(b'\n')), 2)

    @async_test
    async def test_field_delete(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        await doc.ff.put(fcontents)
        self.assertTrue(doc.ff.grid_id)
        await doc.ff.delete()
        self.assertIsNone(doc.ff.grid_id)

    @async_test
    async def test_field_replace(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        await doc.ff.put(fcontents)
        await doc.ff.replace(b'other content')
        self.assertEqual((await doc.ff.read()), b'other content')
