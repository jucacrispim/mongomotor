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
import functools
import greenlet
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass
from mongoengine.context_managers import switch_db as me_switch_db
from motor.metaprogramming import MotorAttributeFactory
from pymongo.database import Database
from mongomotor import utils
from mongomotor.connection import DEFAULT_CONNECTION_NAME
from mongomotor.exceptions import ConfusionError
from mongomotor.monkey import MonkeyPatcher


def asynchronize(method, cls_meth=False):
    """Decorates `method` so it returns a Future.

    The method runs on a child greenlet and resolves the Future when the
    greenlet completes.

    :param method: A mongoengine method to be asynchronized.
    :param cls_meth: Indicates if the method being asynchronized is
       a class method."""

    def async_method(instance_or_class, *args, **kwargs):
        framework = get_framework(instance_or_class)
        loop = framework.get_event_loop()
        future = framework.get_future(loop)

        def call_method():
            # this call_method runs on a child greenlet.

            try:
                result = method(instance_or_class, *args, **kwargs)
                framework.call_soon(
                    loop, functools.partial(future.set_result, result))
            except Exception as e:
                # we can't have StopIteration raised into Future, so
                # we raise StopAsyncIteration
                if isinstance(e, StopIteration):
                    e = StopAsyncIteration(str(e))

                framework.call_soon(
                    loop, functools.partial(future.set_exception, e))

        greenlet.greenlet(call_method).switch()
        # when a I/O operation is started the control will be back to
        # this greenlet and the future will be returned. When the I/O is
        # completed the control goes back to the child greenlet and the future
        # will be resolved.
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


def get_framework(obj):
    """Returns an asynchronous framework for a given object."""

    if hasattr(obj, '_get_db'):
        framework = obj._get_db()._framework

    elif hasattr(obj, 'document_type'):
        framework = obj.document_type._get_db()._framework

    elif hasattr(obj, '_document'):
        framework = obj._document._get_db()._framework

    elif hasattr(obj, 'owner_document'):
        framework = obj.owner_document._get_db()._framework

    elif hasattr(obj, 'instance'):
        framework = obj.instance._get_db()._framework

    else:
        raise ConfusionError('Don\'t know how to get framework for {}'.format(
            str(obj)))

    return framework


def get_future(obj):
    """Returns a future for a given object"""
    framework = get_framework(obj)
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


class AsyncDocumentMetaclass(AsyncWrapperMetaclass, TopLevelDocumentMetaclass):
    """Metaclass for top level documents that have asynchronous methods."""


class AsyncGenericMetaclass(AsyncWrapperMetaclass):
    """Metaclass for any type of documents that use MotorAttributeFactory."""
