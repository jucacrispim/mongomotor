# -*- coding: utf-8 -*-

from tornado import gen
from mongoengine.base import fields
from mongoengine.common import _import_class
from mongoengine.base.datastructures import BaseDict, BaseList

class ComplexBaseField(fields.ComplexBaseField):
    """
    ComplexBaseField that uses motor

    Handles complex fields, such as lists / dictionaries.

    Allows for nesting of embedded documents inside complex types.
    Handles the lazy dereferencing of a queryset by lazily dereferencing all
    items in a list / dict rather than one at a time.

    .. versionadded:: 0.5
    """

    field = None

    @gen.coroutine
    def __get__(self, instance, owner):
        """Descriptor to automatically dereference references.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        ReferenceField = _import_class('ReferenceField')
        GenericReferenceField = _import_class('GenericReferenceField')
        dereference = (self._auto_dereference and
                       (self.field is None or isinstance(self.field,
                        (GenericReferenceField, ReferenceField))))

        _dereference = _import_class("DeReference")()

        self._auto_dereference = instance._fields[self.name]._auto_dereference
        if (instance._initialised and dereference
            and instance._data.get(self.name)):
            instance._data[self.name] = yield _dereference(
                instance._data.get(self.name), max_depth=1, instance=instance,
                name=self.name
            )
        value = yield super(ComplexBaseField, self).__get__(instance, owner)
        # Convert lists / values so we can watch for any changes on them
        if (isinstance(value, (list, tuple)) and
           not isinstance(value, BaseList)):
            value = BaseList(value, instance, self.name)
            instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, instance, self.name)
            instance._data[self.name] = value

        if (self._auto_dereference and instance._initialised and
           isinstance(value, (BaseList, BaseDict))
           and not value._dereferenced):
            value = yield _dereference(
                value, max_depth=1, instance=instance, name=self.name
            )
            value._dereferenced = True
            instance._data[self.name] = value

        return value
