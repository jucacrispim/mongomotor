# -*- coding: utf-8 -*-

# Copyright 2016-2017 Juca Crispim <juca@poraodojuca.net>

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

from mongoengine import fields
from mongoengine.base.datastructures import (
    BaseDict, BaseList, EmbeddedDocumentList)
from mongoengine.connection import get_db

from motor.metaprogramming import create_class_with_framework
from mongomotor import EmbeddedDocument, gridfs
from mongomotor.metaprogramming import (asynchronize, Async, get_future,
                                        AsyncGenericMetaclass)

from mongoengine.fields import *  # noqa f403 for the sake of the api


class BaseAsyncReferenceField:
    """Base class to asynchronize reference fields."""

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


class ReferenceField(BaseAsyncReferenceField, fields.ReferenceField):
    """A reference to a document that will be automatically dereferenced on
    access (lazily).

    Use the `reverse_delete_rule` to handle what should happen if the document
    the field is referencing is deleted.  EmbeddedDocuments, DictFields and
    MapFields does not support reverse_delete_rule and an
    `InvalidDocumentError` will be raised if trying to set on one of these
    Document / Field types.

    The options are:

      * DO_NOTHING (0)  - don't do anything (default).
      * NULLIFY    (1)  - Updates the reference to null.
      * CASCADE    (2)  - Deletes the documents associated with the reference.
      * DENY       (3)  - Prevent the deletion of the reference object.
      * PULL       (4)  - Pull the reference from a
        :class:`~mongomotor.fields.ListField` of references

    Alternative syntax for registering delete rules (useful when implementing
    bi-directional delete rules)

    .. code-block:: python

        class Bar(Document):
            content = StringField()
            foo = ReferenceField('Foo')

        Foo.register_delete_rule(Bar, 'foo', NULLIFY)

    """

    pass


class GenericReferenceField(BaseAsyncReferenceField, fields.
                            GenericReferenceField):
    pass


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
            # we only need to asynchronize this if we we are not in a
            # child greenlet
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


class GridFSProxy(fields.GridFSProxy, metaclass=AsyncGenericMetaclass):

    delete = Async()
    new_file = Async()
    put = Async()
    read = Async()
    replace = Async()
    close = Async()

    def __getattr__(self, name):
        attrs = ('_fs', 'grid_id', 'key', 'instance', 'db_alias',
                 'collection_name', 'newfile', 'gridout', 'fs')
        if name in attrs:
            return self.__getattribute__(name)
        obj = self.get()
        if hasattr(obj, name):
            return getattr(obj, name)
        raise AttributeError

    @property
    def fs(self):
        if not self._fs:
            db = get_db(self.db_alias)
            grid_class = create_class_with_framework(
                gridfs.MongoMotorAgnosticGridFS, db._framework,
                'mongomotor.gridfs')

            self._fs = grid_class(db, self.collection_name)

        return self._fs

    def write(self, string):
        if self.grid_id:
            if not self.newfile:
                raise GridFSError(  # noqa f405
                    'This document already has a file. Either '
                    'delete it or call replace to overwrite it')

        def new_file_cb(new_file_future):
            self.newfile.write(string)

        new_future = self.new_file()
        new_future.add_done_callback(new_file_cb)
        return new_future

    def replace(self, file_obj, **kwargs):
        del_future = self.delete()

        ret_future = get_future(self)

        def del_cb(del_future):
            put_future = self.put(file_obj, **kwargs)

            def put_cb(put_future):
                result = put_future.result()
                ret_future.set_result(result)

            put_future.add_done_callback(put_cb)

        del_future.add_done_callback(del_cb)

        return ret_future


class FileField(fields.FileField):

    proxy_class = GridFSProxy
