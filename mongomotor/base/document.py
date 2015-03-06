# -*- coding: utf-8 -*-

import tornado
from tornado import gen
from bson.son import SON
from mongoengine.base import document
from mongoengine.common import _import_class
from mongoengine import signals
from mongomotor.fields import ReferenceField


class BaseDocumentMotor(document.BaseDocument):
    def __init__(self, *args, **values):
        """
        Initialise a document or embedded document

        :param __auto_convert: Try and will cast python objects to Object types
        :param values: A dictionary of values for the document
        """
        if args:
            # Combine positional arguments with named arguments.
            # We only want named arguments.
            field = iter(self._fields_ordered)
            # If its an automatic id field then skip to the first defined field
            if self._auto_id_field:
                next(field)
            for value in args:
                name = next(field)
                if name in values:
                    raise TypeError("Multiple values for keyword argument '" + name + "'")
                values[name] = value
        __auto_convert = values.pop("__auto_convert", True)
        signals.pre_init.send(self.__class__, document=self, values=values)

        self._data = {}
        self._dynamic_fields = SON()

        # Assign default values to instance
        for key, field in self._fields.items():
            if self._db_field_map.get(key, key) in values \
               or isinstance(field, ReferenceField):
                continue
            value = getattr(self, key, None)
            setattr(self, key, value)

        # Set passed values after initialisation
        if self._dynamic:
            dynamic_data = {}
            for key, value in values.items():
                if key in self._fields or key == '_id':
                    setattr(self, key, value)
                elif self._dynamic:
                    dynamic_data[key] = value
        else:
            FileField = _import_class('FileField')
            for key, value in values.items():
                if key == '__auto_convert':
                    continue
                key = self._reverse_db_field_map.get(key, key)
                if key in self._fields or key in ('id', 'pk', '_cls'):
                    if __auto_convert and value is not None:
                        field = self._fields.get(key)
                        if field and not isinstance(field, FileField):
                            value = field.to_python(value)
                    setattr(self, key, value)
                else:
                    self._data[key] = value

        # Set any get_fieldname_display methods
        self._BaseDocument__set_field_display()

        if self._dynamic:
            self._dynamic_lock = False
            for key, value in dynamic_data.items():
                setattr(self, key, value)

        # Flag initialised
        self._initialised = True
        signals.post_init.send(self.__class__, document=self)

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
