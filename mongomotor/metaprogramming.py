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
import greenlet
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass
from motor.metaprogramming import MotorAttributeFactory
from mongomotor.exceptions import ConfusionError


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
                framework.call_soon(
                    loop, functools.partial(future.set_exception, e))

        greenlet.greenlet(call_method).switch()
        # when a I/O operation is started the control will be back to
        # this greenlet and the future will be returned. When the I/O is
        # completed the control goes back to the child greenlet and the future
        # will be resolved.
        return future

    if cls_meth:
        async_method = classmethod(async_method)

    return functools.wraps(method)(async_method)


def get_framework(obj):
    """Returns a asynchronous framework for a given object."""

    if hasattr(obj, '_get_db'):
        framework = obj._get_db()._framework

    elif hasattr(obj, 'document_type'):
        framework = obj.document_type._get_db()._framework

    elif hasattr(obj, '_document'):
        framework = obj._document._get_db()._framework

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


class OriginalDelegate(MotorAttributeFactory):

    """A descriptor that wraps a Motor method, such as insert or remove
    and returns the original PyMongo method. It still uses motor pool and
    event classes so it needs to run in a child greenlet.

    This is done  because I want to be able to asynchronize a method that
    connects to database but I want the method asynchronized returns a
    future not the pymongo motor asynchronized method. Usually is
    complementary to :class:`~mongomotor.metaprogramming.Async`.
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

    def create_attribute(self, cls, attr_name):
        method = None
        # Tries to get the real method from the super classes
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
        return asynchronize(method, cls_meth=self.cls_meth)


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
