# -*- coding: utf-8 -*-

import tornado
from tornado import gen
from bson.son import SON
from mongoengine.base import document
from mongoengine.common import _import_class
from mongoengine import signals
from mongomotor.fields import ReferenceField, ListField


class BaseDocumentMotor(document.BaseDocument):

    def __init__(self, *args, **values):
        orig_fields = self._fields.copy()
        super().__init__(*args, **values)
        for key, field in orig_fields.items():
            if isinstance(field, ReferenceField):
                delattr(self, key)
                continue

            value = getattr(self, key, None)
            setattr(self, key, value)

    @gen.coroutine
    def validate(self, clean=True):
        # Get a list of tuples of field names and their current values
        fields = [(name, self._data.get(name))
                  for name in self._fields_ordered]

        for field, value in fields:
            if isinstance(value, tornado.concurrent.Future):
                value = yield value
                self._data[field] = value
        super(BaseDocumentMotor, self).validate(clean=clean)
