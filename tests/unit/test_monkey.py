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
from mongoengine import connection as me_connection
from mongomotor import monkey, Document, metaprogramming
from mongomotor.connection import connect, disconnect


class MonkeyPatcherTest(TestCase):

    def tearDown(self):
        disconnect()
        super().tearDown()

    def test_patch_db_clients(self):
        client = Mock()
        replicaset_client = Mock()
        with monkey.MonkeyPatcher() as patcher:
            patcher.patch_db_clients(client, replicaset_client)
            self.assertEqual(client, me_connection.MongoClient)

        self.assertNotEqual(replicaset_client,
                            me_connection.MongoReplicaSetClient)

    def test_patch_async_connections(self):
        # here we create one mongomotor connection
        with monkey.MonkeyPatcher() as p:
            p.patch_sync_connections()
            connect()

            with monkey.MonkeyPatcher() as patcher:
                # the patcher will remove all mongomotor connections
                patcher.patch_async_connections()

                self.assertEqual(len(me_connection._connections), 0)

            self.assertEqual(len(me_connection._connections), 1)

    def test_patch_sync_connections(self):
        # here we create one mongomotor connection
        connect()

        class TestClass(Document):

            @metaprogramming.synchronize
            def some_method(self):
                self._get_collection()

        # here we create a sync connection
        TestClass().some_method()
        with monkey.MonkeyPatcher() as patcher:
            # the patcher will remove all pymongo connections
            patcher.patch_sync_connections()

            self.assertEqual(len(me_connection._connections), 1)

        self.assertEqual(len(me_connection._connections), 2)

    def test_patch_item_without_undo(self):
        something = Mock()
        something.attr = 'Bla'

        with monkey.MonkeyPatcher() as patcher:
            patcher.patch_item(something, 'attr', 'ble', undo=False)

        self.assertEqual(something.attr, 'ble')
