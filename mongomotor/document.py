# -*- coding: utf-8 -*-

# Copyright 2016 Juca Crispim <juca@poraodojuca.net>

# This file is part of mongomotor.

# mongomotor is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# mongomotor is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with mongomotor. If not, see <http://www.gnu.org/licenses/>.

import pymongo
from pymongo.read_preferences import ReadPreference
from bson.dbref import DBRef
import tornado
from tornado import gen
from mongoengine.queryset import OperationError
from mongoengine import (Document as DocumentBase,
                         EmbeddedDocument,
                         DynamicDocument as DynamicDocumentBase)
from mongoengine.document import _import_class, includes_cls
from mongomotor.base.metaclasses import MapReduceDocumentMetaclass
from mongomotor.fields import ReferenceField, ComplexBaseField
from mongomotor.metaprogramming import AsyncDocumentMetaclass, Async, Sync
from mongomotor.queryset import QuerySet


class Document(DocumentBase, metaclass=AsyncDocumentMetaclass):
    """
    Document version that uses motor mongodb driver.
    It's a copy of some mongoengine.Document methods
    that uses tornado.gen.coroutine and yield things
    to use motor with generator style.
    """

    # setting it here so mongoengine will be happy even if I don't
    # use TopLevelDocumentMetaclass.
    meta = {'abstract': True,
            'max_documents': None,
            'max_size': None,
            'ordering': [],
            'indexes': [],
            'id_field': None,
            'index_background': False,
            'index_drop_dups': False,
            'index_opts': None,
            'delete_rules': None,
            'allow_inheritance': None,
            'queryset_class': QuerySet}

    # Methods that will run asynchronally  and return a future
    save = Async()
    delete = Async()
    modify = Async()
    update = Async()
    compare_indexes = Async(cls_meth=True)
    ensure_indexes = Sync(cls_meth=True)
    ensure_index = Sync(cls_meth=True)

    def __init__(self, *args, **kwargs):
        # we put reference fields in __only_fields because if not
        # we end with futures as default values for references

        only_fields = kwargs.get('__only_fields', [])
        for name, field in self._fields.items():
            if isinstance(field, ReferenceField) or (
                    isinstance(field, ComplexBaseField) and
                    isinstance(field.field, ReferenceField)):
                only_fields.append(name)

        kwargs['__only_fields'] = only_fields
        super().__init__(*args, **kwargs)

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        cls._collection = None
        db = cls._get_db()
        return db.drop_collection(cls._get_collection_name())

    # @classmethod
    # @gen.coroutine
    # def compare_indexes(cls):
    #     """ Compares the indexes defined in MongoEngine with the ones existing
    #     in the database. Returns any missing/extra indexes.
    #     """
    #     required = cls.list_indexes()
    #     existing = [
    #         info['key'] for info in
    #         list((yield cls._get_collection().index_information()).values())]

    #     missing = [index for index in required if index not in existing]
    #     extra = [index for index in existing if index not in required]

    #     # if { _cls: 1 } is missing, make sure it's *really* necessary
    #     if [('_cls', 1)] in missing:
    #         cls_obsolete = False
    #         for index in existing:
    #             if includes_cls(index) and index not in extra:
    #                 cls_obsolete = True
    #                 break
    #         if cls_obsolete:
    #             missing.remove([('_cls', 1)])

    #     return {'missing': missing, 'extra': extra}

    @gen.coroutine
    def reload(self, max_depth=1):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        .. versionchanged:: 0.6  Now chainable
        """
        if not self.pk:
            raise self.DoesNotExist("Document does not exist")
        obj = (yield (yield self._qs.read_preference(ReadPreference.PRIMARY).filter(
            **self._object_key).limit(1)).select_related(max_depth=max_depth))

        if obj:
            obj = obj[0]
        else:
            raise self.DoesNotExist("Document does not exist")

        for field in self._fields_ordered:
            if isinstance(obj[field], tornado.concurrent.Future):

                # this will mark document as changed in this field but this is not
                # the desired behavior, so we'll remove this mark after.
                f = yield self._load_related(obj[field], max_depth - 1)
                obj[field] = f
                # obj._changed_fields.pop(obj._changed_fields.index(field))
                setattr(self, field, self._reload(field, f))
            else:
                setattr(self, field, self._reload(field, obj[field]))

        self._changed_fields = obj._changed_fields
        self._created = False
        return obj

    @gen.coroutine
    def _load_related(self, field, max_depth):
        field = yield field
        return field
        # if max_depth == 0:
        #     return field
        # else:
        #     for f in dir(field):


class DynamicDocument(Document, DynamicDocumentBase,
                      metaclass=AsyncDocumentMetaclass):

    meta = {'abstract': True,
            'max_documents': None,
            'max_size': None,
            'ordering': [],
            'indexes': [],
            'id_field': None,
            'index_background': False,
            'index_drop_dups': False,
            'index_opts': None,
            'delete_rules': None,
            'allow_inheritance': None}

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        DynamicDocumentBase.__delattr__(self, *args, **kwargs)


class MapReducedDocument(DynamicDocument, metaclass=AsyncDocumentMetaclass):
    """This MapReduceDocument is different from the mongoengine's one
    because its intent is to allow you to query over."""
