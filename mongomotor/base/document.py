# -*- coding: utf-8 -*-

from mongoengine.base import document
from mongomotor.fields import ReferenceField


class BaseDocumentMotor(document.BaseDocument):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        for key, field in self._fields.items():
            # Doing thing so I retrieve a actual document in a referrence
            # not a future.
            # see test_get_reference_after_get
            if isinstance(field, ReferenceField):
                delattr(self, key)
