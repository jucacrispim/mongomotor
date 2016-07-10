# -*- coding: utf-8 -*-

from mongoengine.base import document
from mongoengine.fields import ReferenceField


class BaseDocumentMotor(document.BaseDocument):

    def __init__(self, *args, **kwargs):
        only_fields = kwargs.get('__only_fields', [])
        for name, field in self._fields.items():
            if isinstance(field, ReferenceField):
                only_fields.append(name)
        kwargs['__only_fields'] = only_fields
        super().__init__(*args, **kwargs)
