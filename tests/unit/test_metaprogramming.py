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

from asyncio.futures import Future
from unittest import TestCase
from unittest.mock import Mock
from motor.metaprogramming import create_class_with_framework
from motor.frameworks import asyncio as asyncio_framework
from mongomotor import metaprogramming
from tests import async_test


class SyncTest(TestCase):

    def test_create_attribute(self):
        class TestDelegate:

            def a(self):
                pass

        class TestClass:
            __motor_class_name__ = 'MyDelegateTest'
            __delegate_class__ = TestDelegate

            a = metaprogramming.Sync()

        tc = create_class_with_framework(TestClass, asyncio_framework,
                                         self.__module__)
        self.assertEqual(tc.a, TestDelegate.a)


class AsynchonizeTest(TestCase):

    @async_test
    def test_asynchornize(self):

        test_mock = Mock()

        class TestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

            @metaprogramming.asynchronize
            def sync(self):
                test_mock()

        testobj = TestClass()
        self.assertTrue(isinstance(testobj.sync(), Future))
        yield from testobj.sync()
        self.assertTrue(test_mock.called)

    @async_test
    def test_asynchornize_with_exception(self):

        class TestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

            @metaprogramming.asynchronize
            def sync(self):
                raise Exception

        testobj = TestClass()
        with self.assertRaises(Exception):
            yield from testobj.sync()

    def test_get_framework_with_get_db(self):

        class TestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

        self.assertEqual(metaprogramming.get_framework(TestClass()),
                         asyncio_framework)

    def test_get_framework_with_error(self):

        class TestClass:
            pass

        with self.assertRaises(metaprogramming.ConfusionError):
            metaprogramming.get_framework(TestClass())


class AsyncTest(TestCase):

    @async_test
    def test_create_attribute(self):

        test_mock = Mock()

        class BaseTestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

            def some_method(self):
                test_mock()

        class TestClass(BaseTestClass):

            some_method = metaprogramming.Async()

        test_class = TestClass
        test_class.some_method = TestClass.some_method.create_attribute(
            TestClass, 'some_method')

        test_instance = test_class()
        yield from test_instance.some_method()
        self.assertTrue(test_mock.called)

    @async_test
    def test_create_attribute_with_attribute_error(self):

        test_mock = Mock()

        class BaseTestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

            def some_method(self):
                test_mock()

        class TestClass(BaseTestClass):

            some_method = metaprogramming.Async()

        test_class = TestClass
        with self.assertRaises(AttributeError):
            test_class.some_method = TestClass.some_method.create_attribute(
                TestClass, 'some_other_method')


class AsyncDocumentMetaclassTest(TestCase):

    @async_test
    def test_create_attributes(self):

        test_mock = Mock()

        class BaseTestDoc:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

            @classmethod
            def _build_index_specs(cls, indexes):
                pass

            def some_method(self):
                test_mock()

            def meth(self):
                test_mock()

        class TestDoc(BaseTestDoc,
                      metaclass=metaprogramming.AsyncDocumentMetaclass):

            meta = {'abstract': True,
                    'max_documents': None,
                    'max_size': None,
                    'ordering': [],
                    'indexes': [],
                    'id_field': None,
                    'index_background': False,
                    'index_drop_dups': False,
                    'index_opts': None,
                    'delete_rules': None,
                    'allow_inheritance': None}

            meth = metaprogramming.Async()

        test_instance = TestDoc()
        yield from test_instance.meth()
        self.assertTrue(test_mock.called)