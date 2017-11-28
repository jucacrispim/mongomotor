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
from unittest.mock import Mock
from motor.frameworks import asyncio as asyncio_framework
from motor.metaprogramming import create_class_with_framework
from mongomotor import Document, disconnect, EmbeddedDocument
from mongomotor.fields import (ReferenceField, ListField,
                               EmbeddedDocumentField, StringField, DictField,
                               BaseList, BaseDict, GridFSProxy, FileField,
                               GridFSError, GenericReferenceField)
from mongomotor.gridfs import MongoMotorAgnosticGridFS
from tests import async_test, connect2db


class TestReferenceField(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_get(self):
        class RefClass(Document):

            @classmethod
            def _get_db(self):
                db = Mock()
                db._framework = asyncio_framework
                return db

        class SomeClass(Document):
            ref = ReferenceField(RefClass)

        someclass = SomeClass()
        # ref should be a future
        self.assertTrue(hasattr(someclass.ref, 'set_result'))

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
    def test_reference_field_in_a_embedded_field(self):
        try:
            class RefClass(Document):
                pass

            class Embed(EmbeddedDocument):
                ref = ReferenceField(RefClass)

            class TestDocument(Document):
                embed = EmbeddedDocumentField(Embed)

            r = RefClass()
            yield from r.save()
            embed = Embed(ref=r)
            d = TestDocument(embed=embed)
            yield from d.save()

            d = yield from TestDocument.objects.get(id=d.id)
            self.assertTrue((yield from d.embed.ref).id)
        finally:
            yield from RefClass.drop_collection()
            yield from TestDocument.drop_collection()


class GenericReferenceFieldTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_get(self):
        class RefClass(Document):

            @classmethod
            def _get_db(self):
                db = Mock()
                db._framework = asyncio_framework
                return db

        class SomeClass(Document):
            ref = GenericReferenceField()

        someclass = SomeClass()
        someclass.ref = RefClass()

        # ref should be a future
        self.assertTrue(hasattr(someclass.ref, 'set_result'))


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
    def tearDown(self):
        yield from self.reference_class.drop_collection()
        yield from self.test_class.drop_collection()
        yield from self.ref_by_embed.drop_collection()
        yield from self.test_embed_ref.drop_collection()

    def test_get_list_field_with_class(self):
        field = self.test_class.list_reference
        self.assertTrue(isinstance(field, ListField))

    @async_test
    def test_get_list_field_with_string(self):
        test_doc = self.test_class(list_field=['a', 'b'])
        yield from test_doc.save()

        test_doc = yield from self.test_class.objects.get(id=test_doc.id)
        self.assertEqual(test_doc.list_field, ['a', 'b'])

    @async_test
    def test_get_list_field_with_embedded(self):
        embed = self.embed(field='bla')
        test_doc = self.reference_class(embed_list=[embed])
        yield from test_doc.save()

        test_doc = yield from self.reference_class.objects.get(id=test_doc.id)
        self.assertEqual(test_doc.embed_list, [embed])

    @async_test
    def test_get_list_field_with_reference(self):
        ref = self.reference_class()
        yield from ref.save()
        test_doc = self.test_class(list_reference=[ref])
        yield from test_doc.save()

        test_doc = yield from self.test_class.objects.get(id=test_doc.id)
        refs = yield from test_doc.list_reference
        self.assertTrue(isinstance(refs[0], self.reference_class))

    @async_test
    def test_get_list_field_with_empyt_references(self):
        """Ensures that a empty list of references returns a empyt list,
        not None."""
        test_doc = self.test_class()
        yield from test_doc.save()

        test_doc = yield from self.test_class.objects.get(id=test_doc.id)
        refs = yield from test_doc.list_reference
        self.assertIsInstance(refs, list)
        self.assertFalse(refs)

    @async_test
    def test_convert_value_with_list(self):
        doc = self.test_class(list_field=[1, 2])
        self.assertIsInstance(doc.list_field, BaseList)

    @async_test
    def test_convert_value_with_dict(self):
        doc = self.test_class(dict_field={'a': 1, 'b': 2})
        self.assertIsInstance(doc.dict_field, BaseDict)

    @async_test
    def test_embedded_list_with_references(self):
        """Ensures that we can retrieve a list of embedded documents
        that has references."""

        ref = self.ref_by_embed()
        yield from ref.save()
        embed = self.embed_ref(ref=ref)
        doc = self.test_embed_ref(embedlist=[embed])
        yield from doc.save()
        doc = yield from self.test_embed_ref.objects.get(id=doc.id)
        self.assertTrue((yield from doc.embedlist[0].ref).id)


class GridFSProxyTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_fs(self):
        proxy = GridFSProxy()
        grid_class = create_class_with_framework(
            MongoMotorAgnosticGridFS, asyncio_framework,
            'mongomotor.gridfs')
        self.assertIsInstance(proxy.fs, grid_class)


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
    def tearDown(self):
        yield from self.test_doc.drop_collection()
        yield from self.test_doc._get_db().fs.files.remove()
        yield from self.test_doc._get_db().fs.chunks.remove()

    @async_test
    def test_file_field_put(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        yield from doc.ff.put(fcontents)
        self.assertTrue(doc.ff.grid_id)

    @async_test
    def test_file_field_read(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        yield from doc.ff.put(fcontents)
        yield from doc.save()
        doc = yield from self.test_doc.objects.get(id=doc.id)
        contents = yield from doc.ff.read()
        self.assertEqual(contents, fcontents)

    @async_test
    def test_new_file(self):
        doc = self.test_doc()
        yield from doc.ff.new_file()
        self.assertTrue(doc.ff.grid_id)

    @async_test
    def test_field_write_with_already_existent_file(self):
        doc = self.test_doc()
        yield from doc.ff.put(b'a file')

        with self.assertRaises(GridFSError):
            yield from doc.ff.write('something')

    @async_test
    def test_field_write(self):
        doc = self.test_doc()
        yield from doc.ff.write(b'a file')
        yield from doc.ff.write(b'\nthe test')
        yield from doc.ff.close()
        content = yield from doc.ff.read()
        self.assertEqual(len(content.split(b'\n')), 2)

    @async_test
    def test_field_delete(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        yield from doc.ff.put(fcontents)
        self.assertTrue(doc.ff.grid_id)
        yield from doc.ff.delete()
        self.assertIsNone(doc.ff.grid_id)

    @async_test
    def test_field_replace(self):
        doc = self.test_doc()
        fcontents = b'some file contents'
        yield from doc.ff.put(fcontents)
        yield from doc.ff.replace(b'other content')
        self.assertEqual((yield from doc.ff.read()), b'other content')
