# -*- coding: utf-8 -*-

import tornado
from tornado import gen
from bson.son import SON
from mongoengine.base import document
from mongoengine.common import _import_class
from mongoengine import signals
from mongoengine.errors import FieldDoesNotExist
from mongoengine.base.datastructures import (
    StrictDict,
    SemiStrictDict
)

from mongomotor.fields import ReferenceField, ListField


class BaseDocumentMotor(document.BaseDocument):

    def __init__(self, *args, **values):
        """
        Initialise a document or embedded document

        :param __auto_convert: Try and will cast python objects to Object types
        :param values: A dictionary of values for the document
        """
        self._initialised = False
        self._created = True
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
                    raise TypeError(
                        "Multiple values for keyword argument '" + name + "'")
                values[name] = value

        __auto_convert = values.pop("__auto_convert", True)

        # 399: set default values only to fields loaded from DB
        __only_fields = set(values.pop("__only_fields", values))

        _created = values.pop("_created", True)

        signals.pre_init.send(self.__class__, document=self, values=values)

        # Check if there are undefined fields supplied, if so raise an
        # Exception.
        if not self._dynamic:
            for var in list(values.keys()):
                if var not in list(self._fields.keys()) + ['id', 'pk', '_cls', '_text_score']:
                    msg = (
                        "The field '{0}' does not exist on the document '{1}'"
                    ).format(var, self._class_name)
                    raise FieldDoesNotExist(msg)

        if self.STRICT and not self._dynamic:
            self._data = StrictDict.create(allowed_keys=self._fields_ordered)()
        else:
            self._data = SemiStrictDict.create(
                allowed_keys=self._fields_ordered)()

        self._data = {}
        self._dynamic_fields = SON()

        # Assign default values to instance
        # MONGOMOTOR HERE! The shit related to
        # futures with reference fields
        for key, field in self._fields.items():
            if self._db_field_map.get(key, key) in __only_fields \
               or isinstance(field, ReferenceField):
                # the whole method for this fucking line!
                continue
            value = getattr(self, key, None)
            setattr(self, key, value)

        if "_cls" not in values:
            self._cls = self._class_name

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
        # shitty __
        self._BaseDocument__set_field_display()

        if self._dynamic:
            self._dynamic_lock = False
            for key, value in dynamic_data.items():
                setattr(self, key, value)

        # Flag initialised
        self._initialised = True
        self._created = _created
        signals.post_init.send(self.__class__, document=self)


    # def __init__(self, *args, **values):
    #     # tragic
    #     orig_fields = self._fields.copy()
    #     super().__init__(*args, **values)

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
