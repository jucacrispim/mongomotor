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


from mongoengine import (Document as DocumentBase,
                         DynamicDocument as DynamicDocumentBase)
from mongomotor.fields import ReferenceField, ComplexBaseField
from mongomotor.metaprogramming import AsyncDocumentMetaclass, Async, Sync
from mongomotor.queryset import QuerySet


class Document(DocumentBase, metaclass=AsyncDocumentMetaclass):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongomotor.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongomotor.Document` subclass will be the name of the subclass
    converted to lowercase. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongomotor.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To disable this behaviour and remove the dependence on the presence of
    `_cls` set :attr:`allow_inheritance` to ``False`` in the :attr:`meta`
    dictionary.

    A :class:`~mongomotor.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. :attr:`max_size` is rounded up
    to the next multiple of 256 by MongoDB internally and mongoengine before.
    Use also a multiple of 256 to avoid confusions.  If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10485760 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be enabled by specifying
    :attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    True then indexes will be created by MongoMotor.

    By default, _cls will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting cls to False on the specific index or
    by setting index_cls to False on the meta dictionary for the document.

    By default, any extra attribute existing in stored data but not declared
    in your model will raise a :class:`mongoengine.FieldDoesNotExist` error.
    This can be disabled by setting :attr:`strict` to ``False``
    in the :attr:`meta` dictionary.
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
            'auto_create_index': False,
            'queryset_class': QuerySet}

    # Methods that will run asynchronally  and return a future
    save = Async()
    delete = Async()
    modify = Async()
    update = Async()
    reload = Async()
    compare_indexes = Async(cls_meth=True)
    ensure_indexes = Sync(cls_meth=True)
    ensure_index = Sync(cls_meth=True)

    def __init__(self, *args, **kwargs):
        # The thing here that if we try to dereference
        # references now we end with a future as the attribute so
        # we don't dereference here.
        fields = []
        for name, field in self._fields.items():
            if isinstance(field, ReferenceField) or (
                    isinstance(field, ComplexBaseField) and
                    isinstance(field.field, ReferenceField)):
                fields.append((field, field._auto_dereference))
                field._auto_dereference = False

        super().__init__(*args, **kwargs)
        # and here we back things to normal
        for field, deref in fields:
            field._auto_dereference = deref



    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`mongomotor.Document` type from the database.
        """
        cls._collection = None
        db = cls._get_db()
        return db.drop_collection(cls._get_collection_name())


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


# class MapReducedDocument(DynamicDocument, metaclass=AsyncDocumentMetaclass):
#     """This MapReduceDocument is different from the mongoengine's one
#     because its intent is to allow you to query over."""
