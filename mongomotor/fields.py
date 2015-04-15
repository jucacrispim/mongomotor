# -*- coding: utf-8 -*-

from bson import DBRef
from tornado import gen
from mongoengine import fields
from mongoengine.fields import *


class ReferenceField(fields.ReferenceField):
    @gen.coroutine
    def __get__(self, instance, owner):
        """Descriptor to allow lazy dereferencing.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        value = instance._data.get(self.name)
        self._auto_dereference = True
        # instance._fields[self.name]._auto_dereference

        # Dereference DBRefs
        if self._auto_dereference and isinstance(value, DBRef):
            value = yield self.document_type._get_db().dereference(value)
            if value is not None:
                instance._data[self.name] = self.document_type._from_son(value)

        return super(fields.ReferenceField, self).__get__(instance, owner)
