# -*- coding: utf-8 -*-

import tornado
from tornado import gen
from mongoengine.base import document


class BaseDocumentMotor(document.BaseDocument):

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
