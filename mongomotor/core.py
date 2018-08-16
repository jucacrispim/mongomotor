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

import textwrap
import types
from motor.core import (AgnosticCollection, AgnosticClient, AgnosticDatabase,
                        AgnosticCursor)
from motor.metaprogramming import create_class_with_framework, ReadOnlyProperty
import pymongo
from pymongo.database import Database
from pymongo.collection import Collection
from mongomotor import PY35
from mongomotor.metaprogramming import OriginalDelegate


def _rebound(ret, obj):
    try:
        ret = types.MethodType(ret.__func__, obj)
    except AttributeError:
        pass
    return ret


class MongoMotorAgnosticCursor(AgnosticCursor):

    __motor_class_name__ = 'MongoMotorCursor'

    distinct = OriginalDelegate()
    explain = OriginalDelegate()

    def __init__(self, *args, **kwargs):
        super(AgnosticCursor, self).__init__(*args, **kwargs)

        # here we get the mangled stuff in the delegate class and
        # set here
        attrs = [a for a in dir(self.delegate) if a.startswith('_Cursor__')]
        for attr in attrs:
            setattr(self, attr, getattr(self.delegate, attr))

    # these are used internally. If you try to
    # iterate using for in a main greenlet you will
    # see an exception.
    # To iterate use a queryset and iterate using motor style
    # with fetch_next/next_object
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.delegate)

    def __getitem__(self, index):
        r = self.delegate[index]
        if isinstance(r, type(self.delegate)):
            # If the response is a cursor, transform it into a
            # mongomotor cursor.
            r = type(self)(r, self.collection)
        return r

    if PY35:
        exec(textwrap.dedent("""
        from mongomotor.decorators import aiter_compat
        @aiter_compat
        def __aiter__(self):
            return self

        async def __anext__(self):
            # An optimization: skip the "await" if possible.
            if self._buffer_size() or await self.fetch_next:
                return self.next_object()
            raise StopAsyncIteration()
        """), globals(), locals())


class MongoMotorAgnosticCollection(AgnosticCollection):

    __motor_class_name__ = 'MongoMotorCollection'

    # Using the original delegate method (but with motor pool and event)
    # so I don't get a future as the return value and don't need to work
    # with mongoengine code.
    insert = OriginalDelegate()
    insert_many = OriginalDelegate()
    insert_one = OriginalDelegate()
    save = OriginalDelegate()
    update = OriginalDelegate()
    update_one = OriginalDelegate()
    update_many = OriginalDelegate()
    find_one = OriginalDelegate()
    find_and_modify = OriginalDelegate()
    find_one_and_update = OriginalDelegate()
    find_one_and_delete = OriginalDelegate()
    index_information = OriginalDelegate()

    def __init__(self, database, name, _delegate=None):

        db_class = create_class_with_framework(
            MongoMotorAgnosticDatabase, self._framework, self.__module__)

        if not isinstance(database, db_class):
            raise TypeError("First argument to MongoMotorCollection must be "
                            "MongoMotorDatabase, not %r" % database)

        delegate = _delegate or Collection(database.delegate, name)
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
                    " collection, use collection['%s']." % (
                        self.__class__.__name__, name, name,
                        name))
            return _rebound(ret, self)

        return self[name]

    def __getitem__(self, name):
        collection_class = create_class_with_framework(
            MongoMotorAgnosticCollection, self._framework, self.__module__)

        return collection_class(self.database, self.name + '.' + name)

    def find(self, *args, **kwargs):
        """Create a :class:`MongoMotorAgnosticCursor`. Same parameters as for
        PyMongo's :meth:`~pymongo.collection.Collection.find`.

        Note that ``find`` does not take a `callback` parameter, nor does
        it return a Future, because ``find`` merely creates a
        :class:`MongoMotorAgnosticCursor` without performing any operations
        on the server.
        ``MongoMotorAgnosticCursor`` methods such as
        :meth:`~MongoMotorAgnosticCursor.to_list` or
        :meth:`~MongoMotorAgnosticCursor.count` perform actual operations.
        """
        if 'callback' in kwargs:
            raise pymongo.errors.InvalidOperation(
                "Pass a callback to each, to_list, or count, not to find.")

        cursor = self.delegate.find(*args, **kwargs)
        cursor_class = create_class_with_framework(
            MongoMotorAgnosticCursor, self._framework, self.__module__)

        return cursor_class(cursor, self)


class MongoMotorAgnosticDatabase(AgnosticDatabase):

    __motor_class_name__ = 'MongoMotorDatabase'

    dereference = OriginalDelegate()
    authenticate = OriginalDelegate()

    def __init__(self, client, name, _delegate=None):
        if not isinstance(client, AgnosticClient):
            raise TypeError("First argument to MongoMotorDatabase must be "
                            "a Motor client, not %r" % client)

        self._client = client
        delegate = _delegate or Database(client.delegate, name)
        super(AgnosticDatabase, self).__init__(delegate)

    def __getattr__(self, name):
        if name.startswith('_'):
            # samething. try get from delegate first
            try:
                ret = getattr(self.delegate, name)
            except AttributeError:
                raise AttributeError(
                    "%s has no attribute %r. To access the %s"
                    " collection, use database['%s']." % (
                        self.__class__.__name__, name, name,
                        name))
            return _rebound(ret, self)

        return self[name]

    def __getitem__(self, name):
        collection_class = create_class_with_framework(
            MongoMotorAgnosticCollection, self._framework, self.__module__)

        return collection_class(self, name)

    def eval(self, code, *fields):
        return self.command('eval', code, *fields)


class MongoMotorAgnosticClientBase(AgnosticClient):

    max_write_batch_size = ReadOnlyProperty()

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

            return _rebound(ret, self)

        return self[name]

    def __getitem__(self, name):
        db_class = create_class_with_framework(
            MongoMotorAgnosticDatabase, self._framework, self.__module__)

        return db_class(self, name)


class MongoMotorAgnosticClient(MongoMotorAgnosticClientBase, AgnosticClient):

    __motor_class_name__ = 'MongoMotorClient'
