# -*- coding: utf-8 -*-

from mongoengine.base import document
from mongoengine.fields import ReferenceField


class BaseDocumentMotor(document.BaseDocument):

    def __init__(self, *args, **kwargs):
        for name, field in self._fields.items():
            if isinstance(field, ReferenceField):
                kwargs['__only_fields'].append(name)
        super().__init__(*args, **kwargs)
