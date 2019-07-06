# -*- coding: utf-8 -*-

# Copyright 2016, 2017 Juca Crispim <juca@poraodojuca.net>

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
from mongoengine.base.metaclasses import (
    TopLevelDocumentMetaclass,
    DocumentMetaclass
)
from mongoengine.context_managers import switch_db as me_switch_db
from motor.metaprogramming import MotorAttributeFactory
from pymongo.database import Database
from mongomotor import utils
from mongomotor.connection import DEFAULT_CONNECTION_NAME
from mongomotor.exceptions import ConfusionError
from mongomotor.monkey import MonkeyPatcher


def asynchronize(method, cls_meth=False):
    """Decorates `method` so it returns a Future.

    :param method: A mongoengine method to be asynchronized.
    :param cls_meth: Indicates if the method being asynchronized is
       a class method."""

    # If we are not in the main thread, things are already asynchronized
    # so we don't need to asynchronize it again.
    if not utils.is_main_thread():
        return method

    def async_method(instance_or_class, *args, **kwargs):
        framework = get_framework(instance_or_class)
        loop = kwargs.pop('_loop', None) or get_loop(instance_or_class)

        future = framework.run_on_executor(
            loop, method, instance_or_class, *args, **kwargs)
        return future

    # for mm_extensions.py (docs)
    async_method.is_async_method = True
    async_method = functools.wraps(method)(async_method)
    if cls_meth:
        async_method = classmethod(async_method)

    return async_method


def synchronize(method, cls_meth=False):
    """Runs method while using the synchronous pymongo driver.

    :param method: A mongoengine method to run using the pymongo driver.
    :param cls_meth: Indicates if the method is a class method."""

    def wrapper(instance_or_class, *args, **kwargs):
        db = instance_or_class._get_db()
        if isinstance(db, Database):
            # the thing here is that a Sync method may be called by another
            # Sync method and if that happens we simply execute method
            r = method(instance_or_class, *args, **kwargs)

        else:
            # here we change the connection to a sync pymongo connection.
            alias = utils.get_alias_for_db(db)
            cls = instance_or_class if cls_meth else type(instance_or_class)
            alias = utils.get_sync_alias(alias)
            with switch_db(cls, alias):
                r = method(instance_or_class, *args, **kwargs)

        return r
    wrapper = functools.wraps(method)(wrapper)
    if cls_meth:
        wrapper = classmethod(wrapper)
    return wrapper


def _get_db(obj):
    """Returns the database connection instance for a given object."""

    if hasattr(obj, '_get_db'):
        db = obj._get_db()

    elif hasattr(obj, 'document_type'):
        db = obj.document_type._get_db()

    elif hasattr(obj, '_document'):
        db = obj._document._get_db()

    elif hasattr(obj, 'owner_document'):
        db = obj.owner_document._get_db()

    elif hasattr(obj, 'instance'):
        db = obj.instance._get_db()

    else:
        raise ConfusionError('Don\'t know how to get db for {}'.format(
            str(obj)))

    return db


def get_framework(obj):
    """Returns an asynchronous framework for a given object."""

    db = _get_db(obj)
    return db._framework


def get_loop(obj):
    """Returns the io loop for a given object"""

    db = _get_db(obj)
    return db.get_io_loop()


def get_future(obj, loop=None):
    """Returns a future for a given object"""

    framework = get_framework(obj)
    if loop is None:
        loop = framework.get_event_loop()
    future = framework.get_future(loop)
    return future


class switch_db(me_switch_db):

    def __init__(self, cls, db_alias):
        """ Construct the switch_db context manager

        :param cls: the class to change the registered db
        :param db_alias: the name of the specific database to use
        """
        self.cls = cls
        self.collection = cls._collection
        self.db_alias = db_alias
        self.ori_db_alias = cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME)
        self.patcher = MonkeyPatcher()

    def __enter__(self):
        """ changes the db_alias, clears the cached collection and
        patches _connections"""
        super().__enter__()
        self.patcher.patch_async_connections()
        return self.cls

    def __exit__(self, t, value, traceback):
        """ Reset the db_alias and collection """
        self.cls._meta["db_alias"] = self.ori_db_alias
        self.cls._collection = self.collection
        self.patcher.__exit__(t, value, traceback)


class OriginalDelegate(MotorAttributeFactory):

    """A descriptor that wraps a Motor method, such as insert or remove
    and returns the original PyMongo method. It still uses motor pool and
    event classes so it needs to run in a child greenlet.

    This is done  because I want to be able to asynchronize a method that
    connects to database but I want to do that in the mongoengine methods,
    the driver methods should work in a `sync` style, in order to not break
    the mongoengine code, but in a child greenlet to handle the I/O stuff.
    Usually is complementary to :class:`~mongomotor.metaprogramming.Async`.
    """

    def create_attribute(self, cls, attr_name):
        return getattr(cls.__delegate_class__, attr_name)


class Async(MotorAttributeFactory):

    """A descriptor that wraps a mongoengine method, such as save or delete
    and returns an asynchronous version of the method. Usually is
    complementary to :class:`~mongomotor.metaprogramming.OriginalDelegate`.
    """

    def __init__(self, cls_meth=False):
        self.cls_meth = cls_meth
        self.original_method = None

    def _get_super(self, cls, attr_name):
        # Tries to get the real method from the super classes
        method = None

        for base in cls.__bases__:
            try:
                method = getattr(base, attr_name)
                # here we use the __func__ stuff because we want to bind
                # it to an instance or class when we call it
                method = method.__func__
            except AttributeError:
                pass

        if method is None:
            raise AttributeError(
                '{} has no attribute {}'.format(cls, attr_name))

        return method

    def create_attribute(self, cls, attr_name):
        self.original_method = self._get_super(cls, attr_name)
        self.async_method = asynchronize(self.original_method,
                                         cls_meth=self.cls_meth)
        return self.async_method


class Sync(Async):
    """A descriptor that wraps a mongoengine method, ensure_indexes
    and runs it using the synchronous pymongo driver.
    """

    def create_attribute(self, cls, attr_name):
        method = self._get_super(cls, attr_name)
        return synchronize(method, cls_meth=self.cls_meth)


class AsyncWrapperMetaclass(type):
    """Metaclass for classes that use MotorAttributeFactory descriptors."""

    def __new__(cls, name, bases, attrs):

        new_class = super().__new__(cls, name, bases, attrs)
        for attr_name, attr in attrs.items():
            if isinstance(attr, MotorAttributeFactory):
                real_attr = attr.create_attribute(new_class, attr_name)
                setattr(new_class, attr_name, real_attr)

        return new_class


class AsyncTopLevelDocumentMetaclass(AsyncWrapperMetaclass,
                                     TopLevelDocumentMetaclass):
    """Metaclass for top level documents that have asynchronous methods."""


class AsyncGenericMetaclass(AsyncWrapperMetaclass):
    """Metaclass for any type of documents that use MotorAttributeFactory."""


class AsyncDocumentMetaclass(AsyncWrapperMetaclass, DocumentMetaclass):
    """Metaclass for documents that use MotorAttributeFactory."""
