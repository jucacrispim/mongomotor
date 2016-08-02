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


def asynchronize(method):
    """Decorates `method` so it returns a Future.

    The method runs on a child greenlet and resolves the Future when the
    greenlet completes.

    :param method: A mongoengine method to be asynchronized."""

    @functools.wraps(method)
    def async_method(self, *args, **kwargs):
        framework = get_framework(self)
        loop = framework.get_event_loop()
        future = framework.get_future(loop)

        def call_method():
            # this call_method runs on a child greenlet.
            try:
                result = method(self, *args, **kwargs)
                framework.call_soon(
                    loop, functools.partial(future.set_result, result))
            except Exception as e:
                framework.call_soon(
                    loop, functools.partial(future.set_exception, e))

        greenlet.greenlet(call_method).switch()
        # when a I/O operation is done the control will be back to
        # this greenlet and the future will be returned. When the I/O is done
        # the control goes back to the child greenlet and the future
        # will be resolved.
        return future
    return async_method


def get_framework(obj):
    """Returns a asynchronous framework for a given object."""

    if hasattr(obj, '_get_db'):
        framework = obj._get_db()._framework

    elif hasattr(obj, 'document_type'):
        framework = obj.document_type._get_db()._framework

    else:
        raise ConfusionError('Don\'t know how to get framework for {}'.format(
            str(obj)))

    return framework


class Sync(MotorAttributeFactory):

    """A descriptor that wraps a Motor method, such as insert or remove
    and returns the original PyMongo method. This is done because I want
    to asynchronize the whole mongoengine method, not only the PyMongo
    method.
    """

    def create_attribute(self, cls, attr_name):
        return getattr(cls.__delegate_class__, attr_name)


class Async(MotorAttributeFactory):

    """A descriptor that wraps a mongoengine method, such as save or delete
    and returns an asynchronous version of the method.
    """

    def create_attribute(self, cls, attr_name):
        method = None
        # Tries to get the real method from the super classes
        for base in cls.__bases__:
            try:
                method = getattr(base, attr_name)
            except AttributeError:
                pass

        if method is None:
            raise AttributeError(
                '{} has no attribute {}'.format(cls, attr_name))
        return asynchronize(method)


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
