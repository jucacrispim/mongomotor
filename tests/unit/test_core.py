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
from unittest.mock import patch, MagicMock, Mock
from motor.metaprogramming import create_class_with_framework
from motor.frameworks import asyncio as asyncio_framework
from pymongo.collection import Collection
from mongomotor import core


class MongoMotorAgnosticCollectionTest(TestCase):

    def test_pymongo_method(self):
        """Ensures that the original pymongo methods are returned
        after the collection is created."""

        coll = create_class_with_framework(core.MongoMotorAgnosticCollection,
                                           asyncio_framework,
                                           self.__module__)

        self.assertEqual(coll.insert, Collection.insert)

    @patch('mongomotor.core.Database', return_value=None)
    @patch('mongomotor.core.Collection')
    def test_getattr_with_under(self, *args, **kwrags):
        connection = Mock(spec=core.MongoMotorAgnosticClient)
        delegate = MagicMock()
        delegate.name = 'blabla'
        connection.delegate = delegate
        name = 'blabla'
        db = create_class_with_framework(core.MongoMotorAgnosticDatabase,
                                         asyncio_framework,
                                         self.__module__)(connection, name)

        coll = create_class_with_framework(core.MongoMotorAgnosticCollection,
                                           asyncio_framework,
                                           self.__module__)
        coll = coll(db, name)

        self.assertEqual(coll._get_write_mode, coll.delegate._get_write_mode)

    @patch('mongomotor.core.Database', return_value=None)
    @patch('mongomotor.core.Collection')
    def test_getitem(self, *args, **kwargs):
        connection = Mock(spec=core.MongoMotorAgnosticClient)
        delegate = MagicMock()
        delegate.name = 'blabla'
        connection.delegate = delegate
        name = 'blabla'
        db = create_class_with_framework(core.MongoMotorAgnosticDatabase,
                                         asyncio_framework,
                                         self.__module__)(connection, name)

        coll = create_class_with_framework(core.MongoMotorAgnosticCollection,
                                           asyncio_framework,
                                           self.__module__)
        coll = coll(db, name)

        other_coll = coll['other']
        self.assertTrue(isinstance(other_coll, type(coll)))

    @patch('mongomotor.core.Database', return_value=None)
    @patch('mongomotor.core.Collection')
    def test_find(self, *args, **kwargs):
        connection = Mock(spec=core.MongoMotorAgnosticClient)
        delegate = MagicMock()
        delegate.name = 'blabla'
        connection.delegate = delegate
        name = 'blabla'
        db = create_class_with_framework(core.MongoMotorAgnosticDatabase,
                                         asyncio_framework,
                                         self.__module__)(connection, name)

        coll = create_class_with_framework(core.MongoMotorAgnosticCollection,
                                           asyncio_framework,
                                           self.__module__)(db, name)
        cursor = create_class_with_framework(core.MongoMotorAgnosticCursor,
                                             asyncio_framework,
                                             self.__module__)
        self.assertIsInstance(coll.find(), cursor)


class MongoMotorAgnosticDatabaseTest(TestCase):

    @patch('mongomotor.core.Database', return_value=None)
    @patch('mongomotor.core.Collection', return_value=None)
    def test_get_item(self, *args, **kwargs):
        connection = Mock(spec=core.MongoMotorAgnosticClient)
        delegate = MagicMock()
        connection.delegate = delegate
        name = 'blabla'
        db = create_class_with_framework(core.MongoMotorAgnosticDatabase,
                                         asyncio_framework,
                                         self.__module__)(connection, name)

        comp_coll = create_class_with_framework(
            core.MongoMotorAgnosticCollection,
            asyncio_framework,
            self.__module__)

        coll = db['bla']
        self.assertTrue(isinstance(coll, comp_coll))


class MongoMotorAgnosticClientTest(TestCase):

    def test_get_item(self):
        client = create_class_with_framework(core.MongoMotorAgnosticClient,
                                             asyncio_framework,
                                             self.__module__)()
        db = client['some-db']
        comp_db = create_class_with_framework(
            core.MongoMotorAgnosticDatabase,
            asyncio_framework,
            self.__module__)

        self.assertTrue(isinstance(db, comp_db))

    def test_getattr_with_under(self):
        client = create_class_with_framework(core.MongoMotorAgnosticClient,
                                             asyncio_framework,
                                             self.__module__)()
        self.assertEqual(client._send_message, client.delegate._send_message)
