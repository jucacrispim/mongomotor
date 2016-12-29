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


from mongoengine.connection import (connect as me_connect,
                                    DEFAULT_CONNECTION_NAME,
                                    disconnect as me_disconnect,
                                    register_connection)

from mongomotor import utils
from mongomotor.clients import (MongoMotorAsyncIOClient,
                                MongoMotorTornadoClient)
from mongomotor.monkey import MonkeyPatcher

CLIENTS = {'asyncio': (MongoMotorAsyncIOClient,),
           'tornado': (MongoMotorTornadoClient,)}


def connect(db=None, async_framework='asyncio',
            alias=DEFAULT_CONNECTION_NAME, **kwargs):
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

    clients = CLIENTS[async_framework]
    with MonkeyPatcher() as patcher:
        patcher.patch_db_clients(*clients)
        patcher.patch_sync_connections()
        ret = me_connect(db=db, alias=alias, **kwargs)

    # here we register a connection that will use the original pymongo
    # client and if used will block the process
    sync_alias = utils.get_sync_alias(alias)
    register_connection(sync_alias, db, **kwargs)

    return ret


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    """Disconnects from the database indentified by ``alias``.
    """

    me_disconnect(alias=alias)

    # disconneting sync connection
    sync_alias = utils.get_sync_alias(alias)
    me_disconnect(alias=sync_alias)
