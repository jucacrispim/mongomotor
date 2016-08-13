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

from copy import copy
from mongoengine import connection
from mongoengine import connection
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient


class MonkeyPatcher:

    def __init__(self):
        self.patched = {}
        # if the original patched object is a dict, indicates if
        # we should merge the original dict with the dict existing
        # when leaving the context manager.
        self._update_original_dict = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for obj, patches in self.patched.items():
            for attr, origobj in patches.items():
                if self._update_original_dict:
                    current_obj = getattr(obj, attr)
                    if hasattr(current_obj, 'update'):
                        origobj.update(current_obj)
                setattr(obj, attr, origobj)

    def patch_item(self, obj, attr, newitem, undo=True):
        """Sets ``attr`` in ``obj`` with ``newitem``.
        If not ``undo`` the item will continue patched
        after leaving the context manager"""

        NONE = object()
        olditem = getattr(obj, attr, NONE)
        if undo and olditem is not NONE:
            self.patched.setdefault(obj, {}).setdefault(attr, olditem)
        setattr(obj, attr, newitem)

    def patch_db_clients(self, client, replicaset_client):
        """Patches the db clients used to connect to mongodb.

        :param client: Which client should be used.
        :param replicaset_client: Which client should be used
          for replicasets."""

        self.patch_item(connection, 'MongoClient', client)
        self.patch_item(connection, 'MongoReplicaSetClient', replicaset_client)

    def patch_async_connections(self):
        """Patches mongoengine.connection._connections removing all
        asynchronous connections from there.

        It is used when switching to a synchronous connection to avoid
        mongoengine returning a asynchronous connection with the same
        configuration."""

        connections = copy(connection._connections)
        for alias, conn in connection._connections.items():
            conn = connections[alias]
            if not isinstance(conn, MongoClient) and not isinstance(
                    conn, MongoReplicaSetClient):
                del connections[alias]

        # we merge the connections no in next time we use the
        # sync one we don't need to connect again.
        self._update_original_dict = True
        self.patch_item(connection, '_connections', connections)

    def patch_sync_connections(self):
        """Patches mongoengine.connection._connections removing all
        synchronous connections from there.
        """

        connections = copy(connection._connections)
        for alias, conn in connection._connections.items():
            conn = connections[alias]
            if isinstance(conn, MongoClient) or isinstance(
                    conn, MongoReplicaSetClient):
                del connections[alias]

        self._update_original_dict = True
        self.patch_item(connection, '_connections', connections)
