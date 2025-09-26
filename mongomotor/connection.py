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


import asyncio
from mongoengine import connection
from mongoengine.connection import (connect as me_connect,
                                    DEFAULT_CONNECTION_NAME,
                                    register_connection,
                                    get_connection,
                                    _connection_settings,
                                    _connections,
                                    _dbs)
from pymongo import AsyncMongoClient

from mongomotor import utils
from mongomotor.monkey import MonkeyPatcher

_db_version = {}


def get_mongodb_version(alias=DEFAULT_CONNECTION_NAME):
    """Return the version of the connected mongoDB (first 2 digits)

    :param alias: The alias identifying the connection
    :return: tuple(int, int)
    """
    # e.g: (3, 2)
    version_list = get_connection(alias).server_info()["versionArray"][:2]
    return tuple(version_list)


def get_db_version(alias=DEFAULT_CONNECTION_NAME):
    """Returns the version of the database for a given alias. This
    will patch the original mongoengine's get_mongodb_version.

    :param alias: The alias identifying the connection.
    """

    return _db_version[alias]


def connect(db=None, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases.  Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

    Parameters are the same as for :func:`mongoengine.connection.connect`
    plus one:

    :param async_framework: Which asynchronous framework should be used.
      It can be `tornado` or `asyncio`. Defaults to `asyncio`.

    """
    kwargs['uuidrepresentation'] = 'standard'
    with MonkeyPatcher() as patcher:
        patcher.patch_db_clients(AsyncMongoClient)
        patcher.patch_sync_connections()
        ret = me_connect(db=db, alias=alias, **kwargs)

    # here we register a connection that will use the original pymongo
    # client and if used will block the process.
    # We need to patch here otherwise we will get the async connection
    # beeing reused instead of a sync one.
    with MonkeyPatcher() as patcher:
        patcher.patch_item(connection, '_find_existing_connection',
                           lambda *a, **kw: None)
        kwargs.pop('io_loop', None)
        sync_alias = utils.get_sync_alias(alias)
        register_connection(sync_alias, db, **kwargs)
        _db_version[alias] = get_mongodb_version(sync_alias)
    return ret


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    """Close the connection with a given alias."""
    from mongoengine import Document
    from mongoengine.base.common import _get_documents_by_db

    connection = _connections.pop(alias, None)
    if connection:
        # MongoEngine may share the same MongoClient across multiple aliases
        # if connection settings are the same so we only close
        # the client if we're removing the final reference.
        # Important to use 'is' instead of '==' because clients connected
        # to the same cluster will compare equal even with different options
        if all(connection is not c for c in _connections.values()):
            loop = asyncio.new_event_loop()
            loop.run_until_complete(connection.close())

    if alias in _dbs:
        # Detach all cached collections in Documents
        for doc_cls in _get_documents_by_db(alias, DEFAULT_CONNECTION_NAME):
            if issubclass(doc_cls, Document):  # Skip EmbeddedDocument
                doc_cls._disconnect()

        del _dbs[alias]

    if alias in _connection_settings:
        del _connection_settings[alias]
