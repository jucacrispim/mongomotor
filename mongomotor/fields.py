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

from uuid import uuid4
from mongoengine import fields
from mongoengine.base.datastructures import (
    BaseDict, BaseList, EmbeddedDocumentList)
from mongoengine.connection import get_db

from motor.metaprogramming import create_class_with_framework
from mongoengine.document import EmbeddedDocument
from mongomotor import gridfs
from mongomotor.metaprogramming import (asynchronize, Async,
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.grid_in = None
        self.grid_out = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc, exc_type, exc_tb):
        await self.close()

    @property
    def fs(self):
        if not self._fs:
            db = get_db(self.db_alias)
            grid_class = create_class_with_framework(
                gridfs.MongoMotorAgnosticGridFS, db._framework,
                'mongomotor.gridfs')

            self._fs = grid_class(db, self.collection_name)

        return self._fs

    def new_file(self, **metadata):
        """Opens a new stream for writing to gridfs.

        :param metadata: File's metadata.
        """
        file_name = uuid4().hex
        grid_in = self.fs.open_upload_stream(file_name, metadata=metadata)
        self.grid_id = grid_in._id
        self.grid_in = grid_in
        self._mark_as_changed()
        return self

    async def close(self):
        if self.grid_in:
            await self.grid_in.close()
            self.grid_in = None

    async def write(self, data):
        """Writes ``data`` to gridfs.

        :param data: String or bytes to write to gridfs."""

        if self.grid_id:
            if not self.grid_in:
                raise GridFSError(  # noqa f405
                    'This document already has a file. Either '
                    'delete it or call replace to overwrite it')

        elif not self.grid_in:
            raise GridFSError('You must create a new file first. Call '
                              '``new_file`` or use the async context manager')

        return self.grid_in.write(data)

    async def put(self, data, **metadata):
        """Writes ``data`` to gridfs.

        :param data: byte-string to write.
        :param metatada: File's metadata.
        """

        async with self.new_file(**metadata):
            await self.write(data)
        self._mark_as_changed()

    async def read(self):
        if not self.grid_id:
            return None

        self.grid_out = await self.fs.open_download_stream(self.grid_id)
        r = await self.grid_out.read()
        return r

    async def delete(self):
        # Delete file from GridFS, FileField still remains
        await self.fs.delete(self.grid_id)
        self.grid_in = None
        self.grid_id = None
        self.grid_out = None
        self._mark_as_changed()

    async def replace(self, data, **metadata):
        """Replaces the contents of the file with ``data``.

        :param data: A byte-string to write to gridfs.
        :param metatada: File metadata.
        """
        await self.delete()
        await self.put(data, **metadata)


class FileField(fields.FileField):

    proxy_class = GridFSProxy
