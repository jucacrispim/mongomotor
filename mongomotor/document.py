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
from mongoengine.errors import InvalidDocumentError, InvalidQueryError
from mongomotor.fields import ReferenceField, ComplexBaseField
from mongomotor.metaprogramming import (AsyncDocumentMetaclass, Async, Sync,
                                        get_future)
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
    compare_indexes = Sync(cls_meth=True)
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

    def modify(self, query={}, **update):
        """Perform an atomic update of the document in the database and reload
        the document object using updated version.

        Returns True if the document has been updated or False if the document
        in the database doesn't match the query.

        .. note:: All unsaved changes that have been made to the document are
            rejected if the method returns True.

        :param query: the update will be performed only if the document in the
            database matches the query
        :param update: Django-style update keyword arguments
        """

        if self.pk is None:
            raise InvalidDocumentError(
                "The document does not have a primary key.")

        id_field = self._meta["id_field"]
        query = query.copy() if isinstance(
            query, dict) else query.to_query(self)

        if id_field not in query:
            query[id_field] = self.pk
        elif query[id_field] != self.pk:
            msg = "Invalid document modify query: "
            msg += "it must modify only this document."
            raise InvalidQueryError(msg)

        updated_future = self._qs(**query).modify(new=True, **update)
        ret_future = get_future(self)

        def updated_cb(updated_future):
            try:
                updated = updated_future.result()
                if updated is None:
                    ret_future.set_result(False)
                    return

                for field in self._fields_ordered:
                    setattr(self, field, self._reload(field, updated[field]))

                self._changed_fields = updated._changed_fields
                self._created = False
                ret_future.set_result(True)
                return
            except Exception as e:
                ret_future.set_exception(e)

        updated_future.add_done_callback(updated_cb)
        return ret_future

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`mongomotor.Document` type from the database.
        """
        cls._collection = None
        db = cls._get_db()
        return db.drop_collection(cls._get_collection_name())

    @property
    def _qs(self):
        """
        Returns the queryset to use for updating / reloading / deletions
        """
        if not hasattr(self, '__objects'):
            self.__objects = QuerySet(self, self._get_collection())
        return self.__objects


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
