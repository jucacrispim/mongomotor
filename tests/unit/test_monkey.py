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
from mongomotor import monkey


class MonkeyPatcherTest(TestCase):

    def test_patch_connection(self):
        client = Mock()
        replicaset_client = Mock()
        with monkey.MonkeyPatcher() as patcher:
            patcher.patch_connection(client, replicaset_client)
            self.assertEqual(client, me_connection.MongoClient)

        self.assertNotEqual(replicaset_client,
                            me_connection.MongoReplicaSetClient)
