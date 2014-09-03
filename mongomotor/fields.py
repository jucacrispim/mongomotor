# -*- coding: utf-8 -*-

from bson import DBRef
from tornado import gen
from mongoengine import fields
from mongoengine.fields import (StringField, URLField, EmailField, IntField,
                                LongField, FloatField, DecimalField,
                                BooleanField, DateTimeField, ComplexBaseField,
                                EmbeddedDocumentField,
                                GenericEmbeddedDocumentField,DynamicField,
                                ListField, SortedListField, DictField)
from mongoengine import Document


class ReferenceField(fields.ReferenceField):
    def __init__(self, document_type, dbref=False,
                 reverse_delete_rule=fields.DO_NOTHING, **kwargs):
        """Initialises the Reference Field.

        :param dbref:  Store the reference as :class:`~pymongo.dbref.DBRef`
          or as the :class:`~pymongo.objectid.ObjectId`.id .
        :param reverse_delete_rule: Determines what to do when the referring
          object is deleted
        """
        if not isinstance(document_type, str):
            if not issubclass(document_type, (Document, str)):
                self.error('Argument to ReferenceField constructor must be a '
                           'document class or a string')

        self.dbref = dbref
        self.document_type_obj = document_type
        self.reverse_delete_rule = reverse_delete_rule
        super(fields.ReferenceField, self).__init__(**kwargs)

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
