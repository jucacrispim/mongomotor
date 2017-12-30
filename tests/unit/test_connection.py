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
try:
    import tornado
except ImportError:
    tornado = None

from mongoengine.connection import _connection_settings
from mongomotor import connect, disconnect
from mongomotor.connection import (MongoMotorAsyncIOClient,
                                   MongoMotorTornadoClient)
from mongomotor.clients import DummyMongoMotorTornadoClient


class ConnectionTest(TestCase):

    def tearDown(self):
        disconnect()

    if tornado:
        def test_connect_with_tornado(self):
            conn = connect(async_framework='tornado')
            self.assertTrue(isinstance(conn, MongoMotorTornadoClient))

    @patch('mongomotor.connection.CLIENTS',
           {'tornado': DummyMongoMotorTornadoClient})
    def test_connect_with_tornado_not_installed(self):
        with self.assertRaises(Exception):
            connect(async_framework='tornado')

    def test_connect_with_asyncio(self):
        conn = connect()
        self.assertTrue(isinstance(conn, MongoMotorAsyncIOClient))

    def test_registered_connections(self):
        # ensures that a sync connection was registered
        connect()
        self.assertEqual(len(_connection_settings), 2,
                         _connection_settings.keys())
