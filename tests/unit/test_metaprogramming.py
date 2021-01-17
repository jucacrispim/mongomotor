# -*- coding: utf-8 -*-

# Copyright 2016, 2017 Juca Crispim <juca@poraodojuca.net>

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
from asyncio.futures import Future
from multiprocessing.pool import ThreadPool
from unittest import TestCase
from unittest.mock import Mock
from mongoengine import connection
from motor.metaprogramming import create_class_with_framework
from motor.frameworks import asyncio as asyncio_framework
from mongomotor import metaprogramming, Document, monkey
from mongomotor.connection import disconnect
from tests import async_test, connect2db


class OriginalDelegateTest(TestCase):

    def test_create_attribute(self):
        class TestDelegate:

            def a(self):
                pass

        class TestClass:
            __motor_class_name__ = 'MyDelegateTest'
            __delegate_class__ = TestDelegate

            a = metaprogramming.OriginalDelegate()

        tc = create_class_with_framework(TestClass, asyncio_framework,
                                         self.__module__)
        self.assertEqual(tc.a, TestDelegate.a)


class AsynchonizeTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    @async_test
    async def test_asynchronize(self):

        test_mock = Mock()

        class TestClass(Document):
            meta = {'abstract': True}

            @metaprogramming.asynchronize
            def sync(self):
                test_mock()

        testobj = TestClass()
        self.assertTrue(isinstance(testobj.sync(), Future))
        await testobj.sync()
        self.assertTrue(test_mock.called)

    @async_test
    async def test_asynchronize_not_on_main_thread(self):

        test_mock = Mock()

        def create_instance():
            class TestClass(Document):
                meta = {'abstract': True}

                @metaprogramming.asynchronize
                def sync(self):
                    test_mock()

            return TestClass()

        pool = ThreadPool(processes=1)
        r = pool.apply_async(create_instance)
        testobj = r.get()
        self.assertFalse(isinstance(testobj.sync(), Future))
        testobj.sync()
        pool.close()
        self.assertTrue(test_mock.called)

    @async_test
    async def test_asynchronize_cls(self):

        test_mock = Mock()

        class TestClass(Document):

            meta = {'abstract': True}

            @classmethod
            def sync(cls):
                test_mock()

        TestClass.sync = metaprogramming.asynchronize(TestClass.sync.__func__,
                                                      cls_meth=True)
        self.assertTrue(isinstance(TestClass.sync(), Future))

        await TestClass.sync()
        self.assertTrue(test_mock.called)

    @async_test
    async def test_asynchornize_with_exception(self):

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
            await testobj.sync()

    def test_get_framework_with_get_db(self):

        class TestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

        self.assertEqual(metaprogramming.get_framework(TestClass()),
                         asyncio_framework)

    def test_get_framework_with_owner_document(self):

        class OwnerDoc:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

        class TestClass:
            owner_document = OwnerDoc

        self.assertEqual(metaprogramming.get_framework(TestClass()),
                         asyncio_framework)

    def test_get_framework_with_instance(self):

        class Instance:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

        class TestClass:
            instance = Instance()

        self.assertEqual(metaprogramming.get_framework(TestClass()),
                         asyncio_framework)

    def test_get_framework_with_error(self):

        class TestClass:
            pass

        with self.assertRaises(metaprogramming.ConfusionError):
            metaprogramming.get_framework(TestClass())

    def test_get_loop(self):

        db = Mock()

        class MyDoc:

            @classmethod
            def _get_db(cls):
                db._framework = asyncio_framework
                return db

        loop = metaprogramming.get_loop(MyDoc())
        self.assertTrue(loop)
        self.assertTrue(db.get_io_loop.called)

    def test_get_future(self):
        class TestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

        future = metaprogramming.get_future(TestClass())
        self.assertIsInstance(future, Future)

    @async_test
    async def test_get_future_with_loop(self):
        class TestClass:

            @classmethod
            def _get_db(cls):
                db = Mock()
                db._framework = asyncio_framework
                return db

        loop = asyncio.get_event_loop()
        future = metaprogramming.get_future(TestClass(), loop=loop)
        self.assertIsInstance(future, Future)


class SynchronizeTest(TestCase):

    def tearDown(self):
        disconnect()

    def test_synchronize(self):
        class TestClass(Document):

            @metaprogramming.synchronize
            def some_method(self):
                self._get_collection()

        # as we are testing synchronize, we remove all sync connections
        # so it does not interfere in our test
        with monkey.MonkeyPatcher() as patcher:
            connect2db(async_framework='asyncio')
            patcher.patch_sync_connections()
            # 2 for the sync connection
            self.assertEqual(len(connection._connections), 1)
            TestClass().some_method()
        self.assertEqual(len(connection._connections), 2)


class AsyncTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    @async_test
    async def test_create_attribute(self):

        test_mock = Mock()

        class BaseTestClass(Document):

            meta = {'abstract': True}

            def some_method(self):
                test_mock()

        class TestClass(BaseTestClass):

            some_method = metaprogramming.Async()

        test_class = TestClass

        test_instance = test_class()
        await test_instance.some_method()
        self.assertTrue(test_mock.called)

    @async_test
    async def test_create_class_attribute(self):

        test_mock = Mock()

        class BaseTestClass(Document):

            meta = {'abstract': True}

            @classmethod
            def some_method(cls):
                test_mock()

        class TestClass(BaseTestClass):

            some_method = metaprogramming.Async(cls_meth=True)

        await TestClass.some_method()
        self.assertTrue(test_mock.called)

    @async_test
    async def test_create_attribute_with_attribute_error(self):

        test_mock = Mock()

        class BaseTestClass(Document):

            meta = {'abstract': True}

            def some_method(self):
                test_mock()

        class TestClass(BaseTestClass):

            some_method = metaprogramming.Async()

        test_class = TestClass
        with self.assertRaises(AttributeError):
            test_class.some_method = TestClass.some_method.create_attribute(
                TestClass, 'some_other_method')


class AsyncDocumentMetaclassTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    @async_test
    async def test_create_attributes(self):

        test_mock = Mock()

        class BaseTestDoc(Document):

            meta = {'abstract': True}

            @classmethod
            def _build_index_specs(cls, indexes):
                pass

            def some_method(self):
                test_mock()

            def meth(self):
                test_mock()

        class TestDoc(
                BaseTestDoc,
                metaclass=metaprogramming.AsyncTopLevelDocumentMetaclass):

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
        await test_instance.meth()
        self.assertTrue(test_mock.called)
