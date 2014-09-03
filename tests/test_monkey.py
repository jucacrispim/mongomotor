# -*- coding: utf-8 -*-

import unittest
from mock import Mock, patch
# this module raises an error in atexit. Importing it to mock
from tornado import concurrent
import mongoengine
from mongomotor import monkey


# mocking it to get rid of the atexit error
@patch.object(concurrent, 'futures', Mock())
class MonkeyTest(unittest.TestCase):

    def test_patch_connection(self):
        monkey.patch_connection()

        from mongoengine.connection import get_connection

        self.assertEqual(get_connection.__module__,
                         'mongomotor.connection')

    def test_patch_document(self):
        monkey.patch_document()

        from mongoengine import Document

        self.assertEqual(Document.__module__,
                         'mongomotor.document')

    def test_patch_queryset(self):
        monkey.patch_queryset()

        from mongoengine.base.metaclasses import QuerySetManager

        self.assertEqual(QuerySetManager.__module__,
                         'mongomotor.queryset.manager')

    def test_patch_visitor(self):
        monkey.patch_visitor()

        from mongoengine.queryset.visitor import Q, QueryCompilerVisitor

        self.assertEqual(Q.__module__,
                         'mongomotor.queryset.visitor')

        self.assertEqual(QueryCompilerVisitor.__module__,
                         'mongomotor.queryset.visitor')

    def test_patch_transform(self):
        monkey.patch_transform()

        from mongoengine.queryset.transform import query

        self.assertEqual(query.__module__,
                         'mongomotor.queryset.transform')

    def test_patch_dereference(self):
        monkey.patch_dereference()

        from mongoengine.dereference import DeReference

        self.assertEqual(DeReference.__module__,
                         'mongomotor.dereference')

    def test_patch_fields(self):
        monkey.patch_fields()

        from mongoengine.base.metaclasses import ComplexBaseField
        from mongoengine import ReferenceField

        self.assertEqual(ComplexBaseField.__module__,
                         'mongomotor.base.fields')

        self.assertEqual(ReferenceField.__module__,
                         'mongomotor.fields')

    def test_patch_all(self):
        monkey.patch_all()

        from mongoengine import Document
        from mongoengine.dereference import DeReference
        from mongoengine.connection import get_connection
        from mongoengine.base.metaclasses import QuerySetManager
        from mongoengine.queryset.visitor import Q, QueryCompilerVisitor
        from mongoengine.base.metaclasses import ComplexBaseField
        from mongoengine.queryset.transform import query
        from mongoengine import ReferenceField


        self.assertEqual(get_connection.__module__,
                         'mongomotor.connection')

        self.assertEqual(Document.__module__,
                         'mongomotor.document')

        self.assertEqual(QuerySetManager.__module__,
                         'mongomotor.queryset.manager')

        self.assertEqual(Q.__module__,
                         'mongomotor.queryset.visitor')

        self.assertEqual(QueryCompilerVisitor.__module__,
                         'mongomotor.queryset.visitor')

        self.assertEqual(query.__module__,
                         'mongomotor.queryset.transform')

        self.assertEqual(DeReference.__module__,
                         'mongomotor.dereference')

        self.assertEqual(ComplexBaseField.__module__,
                         'mongomotor.base.fields')

        self.assertEqual(ReferenceField.__module__,
                         'mongomotor.fields')
