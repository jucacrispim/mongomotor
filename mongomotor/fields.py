# -*- coding: utf-8 -*-

# Copyright 2016-2017, 2025 Juca Crispim <juca@poraodojuca.dev>

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
import gridfs
from mongoengine import fields
from mongoengine.base import get_document
from mongoengine.base.datastructures import (
    BaseDict, BaseList, EmbeddedDocumentList)
from mongoengine.common import _import_class
from mongoengine.connection import get_db
from mongoengine.errors import DoesNotExist
from mongoengine.fields import GridFSError


from mongoengine.fields import *  # noqa f403 for the sake of the api


class BaseAsyncReferenceField:
    """Base class for async reference fields."""

    def __get__(self, instance, owner):
        if instance is None:
            return self

        auto_dereference = instance._fields[self.name]._auto_dereference
        if not auto_dereference:
            return instance._data.get(self.name)

        async def get():
            if getattr(instance._data[self.name], "_dereferenced", False):
                return instance._data.get(self.name)

            ref_value = instance._data.get(self.name)
            if isinstance(ref_value, dict) and '_ref' in ref_value.keys():
                ref = ref_value['_ref']
                cls = get_document(ref_value['_cls'])
                instance._data[self.name] = await self._lazy_load_ref(
                    cls, ref)

            elif auto_dereference and isinstance(ref_value, DBRef):
                if hasattr(ref_value, "cls"):
                    # Dereference using the class type specified in the
                    # reference
                    cls = get_document(ref_value.cls)
                else:
                    cls = self.document_type

                instance._data[self.name] = await self._lazy_load_ref(
                    cls, ref_value)
                instance._data[self.name]._dereferenced = True

            return instance._data.get(self.name)

        return get()

    @staticmethod
    async def _lazy_load_ref(ref_cls, dbref):
        dereferenced_son = await ref_cls._get_db().dereference(dbref)
        if dereferenced_son is None:
            raise DoesNotExist(
                f"Trying to dereference unknown document {dbref}")

        return ref_cls._from_son(dereferenced_son)


class ReferenceField(BaseAsyncReferenceField, fields.ReferenceField):
    """A reference to a document that will be dereferenced on
    access.

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


class GenericReferenceField(
        BaseAsyncReferenceField, fields.GenericReferenceField):
    pass


class ComplexBaseField(fields.ComplexBaseField):

    def __get__(self, instance, owner):
        if instance is None:
            return self

        auto_dereference = instance._fields[self.name]._auto_dereference

        dereference = auto_dereference and isinstance(
            self.field, (GenericReferenceField, ReferenceField))
        if not dereference:
            val = instance._data.get(self.name)
            if val is not None:
                self._convert_value(instance, val)
            return instance._data.get(self.name)

        async def get():
            if getattr(instance._data[self.name], "_dereferenced", False):
                return instance._data.get(self.name)

            ref_values = instance._data.get(self.name)
            instance._data[self.name] = await self._lazy_load_refs(
                ref_values=ref_values, instance=instance, name=self.name,
                max_depth=1
            )
            if hasattr(instance._data[self.name], "_dereferenced"):
                instance._data[self.name]._dereferenced = True

            value = instance._data[self.name]

            self._convert_value(instance, value)
            value = instance._data[self.name]
            if (
                auto_dereference
                and instance._initialised
                and isinstance(value, (BaseList, BaseDict))
                and not value._dereferenced
            ):
                value = await self._lazy_load_refs(
                    ref_values=value, instance=instance, name=self.name,
                    max_depth=1
                )
                value._dereferenced = True
                instance._data[self.name] = value

            return value

        return get()

    @staticmethod
    async def _lazy_load_refs(instance, name, ref_values, *, max_depth):
        _dereference = _import_class("DeReference")()
        documents = await _dereference(
            ref_values,
            max_depth=max_depth,
            instance=instance,
            name=name,
        )
        return documents

    def _convert_value(self, instance, value):
        # Convert lists / values so we can watch for any changes on them
        if isinstance(value, (list, tuple)):
            if issubclass(type(self), fields.EmbeddedDocumentListField) \
               and not isinstance(value, EmbeddedDocumentList):
                value = EmbeddedDocumentList(value, instance, self.name)
            elif not isinstance(value, BaseList):
                value = BaseList(value, instance, self.name)
                instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, instance, self.name)
            instance._data[self.name] = value


class EmbeddedDocumentListField(ComplexBaseField,
                                fields.EmbeddedDocumentListField):
    pass


class ListField(ComplexBaseField, fields.ListField):
    pass


class DictField(ComplexBaseField, fields.DictField):
    pass


class GridFSProxy(fields.GridFSProxy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc, exc_type, exc_tb):
        await self.close()

    @property
    def fs(self):
        if not self._fs:
            self._fs = gridfs.AsyncGridFS(
                get_db(self.db_alias), collection=self.collection_name)
        return self._fs

    async def get(self, grid_id=None):
        if grid_id:
            self.grid_id = grid_id

        if self.grid_id is None:
            return None

        try:
            if self.gridout is None:
                self.gridout = await self.fs.get(self.grid_id)
            return self.gridout
        except Exception:
            # File has been deleted
            return None

    async def close(self):
        if self.newfile:
            await self.newfile.close()
            self.newfile = None

    async def write(self, data):
        """Writes ``data`` to gridfs.

        :param data: String or bytes to write to gridfs."""

        if self.grid_id:
            if not self.newfile:
                raise GridFSError(  # noqa f405
                    'This document already has a file. Either '
                    'delete it or call replace to overwrite it')

        else:
            self.new_file()
        await self.newfile.write(data)

    async def put(self, file_obj, **kwargs):
        if self.grid_id:
            raise GridFSError(
                "This document already has a file. Either delete "
                "it or call replace to overwrite it"
            )
        self.grid_id = await self.fs.put(file_obj, **kwargs)
        self._mark_as_changed()

    async def read(self, size=-1):
        gridout = await self.get()
        if gridout is None:
            return None
        else:
            try:
                return await gridout.read(size)
            except Exception:
                return ""

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
