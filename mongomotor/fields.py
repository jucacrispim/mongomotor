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
    BaseDict, BaseList, EmbeddedDocumentList)

from mongoengine.fields import *  # flake8: noqa for the sake of the api
from mongomotor import EmbeddedDocument
from mongomotor.metaprogramming import asynchronize


class ReferenceField(fields.ReferenceField):

    def __get__(self, instance, owner):
        # When we are getting the field from a class not from an
        # instance we don't need a Future
        if instance is None:
            return self

        meth = super().__get__
        if self._auto_dereference:
            if isinstance(instance, EmbeddedDocument):
                # It's used when there's a reference in a EmbeddedDocument.
                # We use it to get the async framework to be used.
                instance._get_db = self.document_type._get_db
            meth = asynchronize(meth)

        return meth(instance, owner)


class ComplexBaseField(fields.ComplexBaseField):

    def __get__(self, instance, owner):

        if instance is None:
            return self

        # The thing here is that I don't want to dereference lists
        # references in embedded documents now. It has the advantage of
        # keeping the same API for embedded documents and references
        # (ie returning a future for references and not a future for
        # embedded documentts) and the disadvantage of not being able to
        # retrieve all references in bulk.
        value = super(fields.ComplexBaseField, self).__get__(instance, owner)
        if isinstance(value, (list, dict, tuple, BaseList, BaseDict)):
            value = self._convert_value(instance, value)
            # It is not in fact dereferenced, we are cheating.
            value._dereferenced = True
        super_meth = super().__get__
        if isinstance(self.field, ReferenceField) and self._auto_dereference:
            r = asynchronize(super_meth)(instance, owner)
        else:
            r = super_meth(instance, owner)

        return r

    def _convert_value(self, instance, value):
        if isinstance(value, (list, tuple)):
            if (issubclass(type(self), fields.EmbeddedDocumentListField) and
                    not isinstance(value, fields.EmbeddedDocumentList)):
                value = EmbeddedDocumentList(value, instance, self.name)
            elif not isinstance(value, BaseList):
                value = BaseList(value, instance, self.name)
            instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, instance, self.name)
            instance._data[self.name] = value

        return value


class ListField(ComplexBaseField, fields.ListField):
    pass


class DictField(ComplexBaseField, fields.DictField):
    pass
