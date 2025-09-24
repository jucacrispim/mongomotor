# -*- coding: utf-8 -*-

# Copyright 2016, 2025 Juca Crispim <juca@poraodojuca.dev>

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
from mongoengine import connection, dereference, signals, context_managers
from mongoengine.queryset import base
from pymongo.mongo_client import MongoClient
from mongomotor import signals as async_signals


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

    def patch_get_mongodb_version(self):
        """Patches mongoengine's get_mongodb_version
        to use a function does not reach the database.
        """

        from .connection import get_db_version
        from mongoengine import fields

        self.patch_item(fields, 'get_mongodb_version',
                        get_db_version)

    def patch_db_clients(self, client):
        """Patches the db clients used to connect to mongodb.

        :param client: Which client should be used."""

        self.patch_item(connection, 'MongoClient', client)

    def patch_async_connections(self):
        """Patches mongoengine.connection._connections removing all
        asynchronous connections from there.

        It is used when switching to a synchronous connection to avoid
        mongoengine returning a asynchronous connection with the same
        configuration."""

        connections = copy(connection._connections)
        for alias, conn in connection._connections.items():
            conn = connections[alias]
            if not isinstance(conn, MongoClient):
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
            if isinstance(conn, MongoClient):
                del connections[alias]

        self._update_original_dict = True
        self.patch_item(connection, '_connections', connections)

    def patch_dereference(self):
        from mongomotor.dereference import MongoMotorDeReference
        self.patch_item(dereference, 'DeReference', MongoMotorDeReference,
                        undo=False)

    def patch_no_dereferencing_active_for_class(self):
        from .context_managers import no_dereferencing_active_for_class
        self.patch_item(context_managers, 'no_dereferencing_active_for_class',
                        no_dereferencing_active_for_class)
        self.patch_item(base, 'no_dereferencing_active_for_class',
                        no_dereferencing_active_for_class)

    def patch_qs_stop_iteration(self):
        """Patches StopIterations raised by mongoengine's queryset
        replacing it by AsyncStopIteration so it can interact well
        with futures."""

        self.patch_item(base, 'StopIteration', StopAsyncIteration, undo=False)
        # self.patch_item(queryset, 'StopIteration', StopAsyncIteration)
        self.patch_item(dereference, 'StopIteration', StopAsyncIteration,
                        undo=False)

    def patch_signals(self):
        """Patches mongoengine signals to use asyncblink signals"""

        self.patch_item(signals, 'pre_init', async_signals.pre_init)
        self.patch_item(signals, 'post_init', async_signals.post_init)
        self.patch_item(signals, 'pre_save', async_signals.pre_save)
        self.patch_item(signals, 'post_save', async_signals.post_save)
        self.patch_item(signals, 'pre_save_post_validation',
                        async_signals.pre_save_post_validation)
        self.patch_item(signals, 'pre_delete', async_signals.pre_delete)
        self.patch_item(signals, 'post_delete', async_signals.post_delete)
        self.patch_item(signals, 'pre_bulk_insert',
                        async_signals.pre_bulk_insert)
        self.patch_item(signals, 'post_bulk_insert',
                        async_signals.post_bulk_insert)
