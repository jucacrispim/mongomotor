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

import functools
from motor import util
from motor.core import (AgnosticCollection, AgnosticClient, AgnosticDatabase,
                        AgnosticClientBase, AgnosticReplicaSetClient)
from motor.metaprogramming import create_class_with_framework
from pymongo.database import Database
from pymongo.collection import Collection
from mongomotor.metaprogramming import Sync


class MongoMotorAgnosticCollection(AgnosticCollection):

    __motor_class_name__ = 'MongoMotorCollection'
    # Making this guys sync because I will asynchronize
    # at mongoengine level.
    insert = Sync()
    save = Sync()
    update = Sync()

    def __init__(self, database, name):

        db_class = create_class_with_framework(
            MongoMotorAgnosticDatabase, self._framework, self.__module__)

        if not isinstance(database, db_class):
            raise TypeError("First argument to MongoMotorCollection must be "
                            "MongoMotorDatabase, not %r" % database)

        delegate = Collection(database.delegate, name)
        super(AgnosticCollection, self).__init__(delegate)
        self.database = database

    def __getattr__(self, name):
        if name.startswith('_'):
            # Here first I try to get the _attribute from
            # from the delegate obj.
            try:
                ret = getattr(self.delegate, name)
            except AttributeError:
                raise AttributeError(
                    "%s has no attribute %r. To access the %s"
                    " collection, use database['%s']." % (
                        self.__class__.__name__, name, name,
                        name))
            return ret

        return self[name]


    def __getitem__(self, name):
        collection_class = create_class_with_framework(
            MongoMotorAgnosticCollection, self._framework, self.__module__)

        return collection_class(self.database, self.name + '.' + name)



class MongoMotorAgnosticDatabase(AgnosticDatabase):

    __motor_class_name__ = 'MongoMotorDatabase'

    def __init__(self, connection, name):
        if not isinstance(connection, AgnosticClientBase):
            raise TypeError("First argument to MongoMotorDatabase must be "
                            "a Motor client, not %r" % connection)

        self.connection = connection
        delegate = Database(connection.delegate, name)
        super(AgnosticDatabase, self).__init__(delegate)

    def __getitem__(self, name):
        collection_class = create_class_with_framework(
            MongoMotorAgnosticCollection, self._framework, self.__module__)

        return collection_class(self, name)



class MongoMotorAgnosticClientBase(AgnosticClientBase):


    def __init__(self, *args, **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        MongoMotorAgnosticClient takes the same constructor arguments as
        :class:`~motor.core.AgnosticClient`:

        """
        if 'io_loop' in kwargs:
            io_loop = kwargs.pop('io_loop')
        else:
            io_loop = self._framework.get_event_loop()

        event_class = functools.partial(util.MotorGreenletEvent, io_loop,
                                        self._framework)
        kwargs['_event_class'] = event_class

        # Our class is not actually AgnosticClient here, it's the version of
        # 'MotorClient' that create_class_with_framework created.
        super(AgnosticClient, self).__init__(io_loop, *args, **kwargs)

    def __getattr__(self, name):
        if name.startswith('_'):
            # the same. Try get from delegate.
            try:
                ret = getattr(self.delegate, name)
            except AttributeError:

                raise AttributeError(
                    "%s has no attribute %r. To access the %s"
                    " database, use client['%s']." % (
                        self.__class__.__name__, name, name, name))
            return ret

        return self[name]

    def __getitem__(self, name):
        db_class = create_class_with_framework(
            MongoMotorAgnosticDatabase, self._framework, self.__module__)

        return db_class(self, name)


class MongoMotorAgnosticClient(MongoMotorAgnosticClientBase, AgnosticClient):

    __motor_class_name__ = 'MongoMotorClient'



class MongoMotorAgnosticReplicaSetClient(MongoMotorAgnosticClientBase,
                                         AgnosticReplicaSetClient):

    __motor_class_name__ = 'MongoMotorReplicaSetClient'
