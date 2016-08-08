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

from bson import DBRef
from tornado import gen
from tornado.concurrent import Future
from mongoengine.common import _import_class
from mongoengine import fields
from mongoengine.base.datastructures import (
    BaseDict, BaseList, EmbeddedDocumentList
)


from mongoengine.fields import *  # flake8: noqa for the sake of the api

from mongomotor.metaprogramming import asynchronize


class ComplexBaseField(fields.ComplexBaseField):

    def __get__(self, instance, owner):
        """Descriptor to automatically dereference references.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        ReferenceField = _import_class('ReferenceField')
        GenericReferenceField = _import_class('GenericReferenceField')
        dereference = (self._auto_dereference and
                       (isinstance(
                           self.field,
                           (GenericReferenceField, ReferenceField))))

        _dereference = _import_class("DeReference")()

        self._auto_dereference = instance._fields[self.name]._auto_dereference
        initialised = instance._initialised
        is_dbref = instance._data.get(self.name) and bool(
            [v for v in instance._data.get(self.name) if isinstance(v, DBRef)])

        if is_dbref or (initialised and dereference):
            @gen.coroutine
            def deref(instance):
                instance._data[self.name] = yield _dereference(
                    instance._data.get(self.name), max_depth=1,
                    instance=instance, name=self.name)

                value = super(fields.ComplexBaseField, self).__get__(
                    instance, owner)
                return self._convert_collections(instance, value)

            return deref(instance)
        else:
            value = super(fields.ComplexBaseField, self).__get__(
                instance, owner)
            return self._convert_collections(instance, value)

    def _convert_collections(self, instance, value):

        # Convert lists / values so we can watch for any changes on them
        EmbeddedDocumentListField = _import_class('EmbeddedDocumentListField')
        if isinstance(value, (list, tuple)):
            if (issubclass(type(self), EmbeddedDocumentListField) and
                    not isinstance(value, EmbeddedDocumentList)):
                value = EmbeddedDocumentList(value, instance, self.name)
            elif not isinstance(value, BaseList):
                value = BaseList(value, instance, self.name)
            instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, instance, self.name)
            instance._data[self.name] = value

        is_refcls = instance._data.get(self.name) and not isinstance(
            instance._data.get(self.name), Future)

        _dereference = _import_class("DeReference")()
        if (self._auto_dereference and instance._initialised and
                isinstance(value, (BaseList, BaseDict)) and
                value and not value._dereferenced and not is_refcls):

            @gen.coroutine
            def deref(instance, value):
                value = yield _dereference(
                    value, max_depth=1, instance=instance, name=self.name
                )
                value._dereferenced = True
                instance._data[self.name] = value
                return value
            return deref(instance, value)

        else:
            return value


class ReferenceField(fields.ReferenceField):

    def __get__(self, instance, owner):
        # When we are getting the field from a class not from an
        # instance we don't need a Future
        if instance is None:
            return self

        super_meth = super().__get__
        return asynchronize(super_meth)(instance, owner)


class ListField(ComplexBaseField, fields.ListField):
    pass


class DictField(ComplexBaseField, fields.DictField):
    pass
