# -*- coding: utf-8 -*-

from bson import DBRef
from tornado import gen
from tornado.concurrent import Future
from mongoengine.common import _import_class
from mongoengine import fields
from mongoengine.base.datastructures import (
    BaseDict, BaseList, EmbeddedDocumentList
)


from mongoengine.fields import *


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

        if is_dbref or (initialised and dereference and
                        instance._data.get(self.name)):
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
        """Descriptor to allow lazy dereferencing.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        value = instance._data.get(self.name)
        # self._auto_dereference = True
        self._auto_dereference = instance._fields[self.name]._auto_dereference

        @gen.coroutine
        def deref(value):
            # Dereference DBRefs
            if self._auto_dereference and isinstance(value, DBRef):
                db = self.document_type._get_db()

                value = yield db.dereference(value)
                if value is not None:
                    instance._data[self.name] = self.document_type._from_son(
                        value)

            return super(fields.ReferenceField, self).__get__(
                instance, owner)
        return deref(value)


class ListField(ComplexBaseField, fields.ListField):
    pass


class DictField(ComplexBaseField, fields.DictField):
    pass
