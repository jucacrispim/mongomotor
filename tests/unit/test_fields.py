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
from unittest.mock import Mock
from motor.frameworks import asyncio as asyncio_framework
from mongomotor import Document, connect, disconnect, EmbeddedDocument
from mongomotor.fields import (ReferenceField, ListField,
                               EmbeddedDocumentField, StringField)
from tests import async_test


class TestReferenceField(TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        connect(db)

    @classmethod
    def tearDownClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        disconnect(db)

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


class TestComplexField(TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        connect(db)

    @classmethod
    def tearDownClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        disconnect(db)

    def setUp(self):

        class Embed(EmbeddedDocument):
            field = StringField()

        class ReferenceClass(Document):
            embed_list = ListField(EmbeddedDocumentField(Embed))

        class TestClass(Document):
            list_field = ListField()
            list_reference = ListField(ReferenceField(ReferenceClass))

        self.embed = Embed
        self.reference_class = ReferenceClass
        self.test_class = TestClass

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
