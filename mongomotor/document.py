# -*- coding: utf-8 -*-

# Copyright 2016, 2025 Juca Crispim <juca@poraodojuca.net>

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

import re
from mongoengine import (Document as DocumentBase,
                         DynamicDocument as DynamicDocumentBase)
from mongoengine.document import (
    EmbeddedDocument as EmbeddedDocumentBase,
    DynamicEmbeddedDocument as DynamicEmbeddedDocumentBase,
    includes_cls,
)
from mongoengine.common import _import_class
from mongoengine.context_managers import set_write_concern
from mongoengine.errors import (
    InvalidDocumentError,
    InvalidQueryError,
    SaveConditionError
)
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass
from mongoengine.queryset import OperationError, NotUniqueError, transform
from mongomotor import signals

from mongomotor.queryset import QuerySet
import pymongo
from pymongo.read_preferences import ReadPreference


class NoDerefInitMixin:
    """A mixin used to Documents and EmbeddedDocuments not to dereference
    reference fields on __init__.
    """

    def __init__(self, *args, **kwargs):

        self._set_no_deref()
        super().__init__(*args, **kwargs)
        self._back_deref()

    def _set_no_deref(self):
        # The thing here is that if we try to dereference
        # references now we end with a future as the attribute so
        # we don't dereference here.
        self._fields_deref = []
        for name, field in self._fields.items():
            self._fields_deref.append((field, field._auto_dereference))
            field._BaseField__auto_dereference = False

    def _back_deref(self):
        # and here we back things to normal
        for field, deref in self._fields_deref:
            field._BaseField__auto_dereference = deref


class Document(NoDerefInitMixin, DocumentBase,
               metaclass=TopLevelDocumentMetaclass):
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

    async def save(
        self,
        force_insert=False,
        validate=True,
        clean=True,
        write_concern=None,
        cascade=None,
        cascade_kwargs=None,
        _refs=None,
        save_condition=None,
        signal_kwargs=None,
        **kwargs,
    ):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created. Returns the saved object instance.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents.
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: (optional) kwargs dictionary to be passed throw
            to cascading saves.  Implies ``cascade=True``.
        :param _refs: A list of processed references used in cascading saves
        :param save_condition: only perform save if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`OperationError` if the conditions are not satisfied
        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.


        """
        signal_kwargs = signal_kwargs or {}

        if self._meta.get("abstract"):
            raise InvalidDocumentError("Cannot save an abstract document.")

        signals.pre_save.send(self.__class__, document=self, **signal_kwargs)

        if validate:
            self.validate(clean=clean)

        if write_concern is None:
            write_concern = {}

        doc_id = self.to_mongo(fields=[self._meta["id_field"]])
        created = "_id" not in doc_id or self._created or force_insert

        signals.pre_save_post_validation.send(
            self.__class__, document=self, created=created, **signal_kwargs
        )
        # it might be refreshed by the pre_save_post_validation hook, e.g.,
        # for etag generation
        doc = self.to_mongo()

        # Initialize the Document's underlying pymongo.Collection
        # (+create indexes) if not already initialized
        # Important to do this here to avoid that the index creation gets
        # wrapped in the try/except block below
        # and turned into mongoengine.OperationError
        if self._collection is None:
            _ = self._get_collection()
        try:
            # Save a new document or update an existing one
            if created:
                object_id = await self._save_create(
                    doc=doc, force_insert=force_insert,
                    write_concern=write_concern
                )
            else:
                object_id, created = await self._save_update(
                    doc, save_condition, write_concern
                )

            if cascade is None:
                cascade = self._meta.get(
                    "cascade", False) or cascade_kwargs is not None

            if cascade:
                kwargs = {
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade,
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs["_refs"] = _refs
                self.cascade_save(**kwargs)

        except pymongo.errors.DuplicateKeyError as err:
            message = "Tried to save duplicate unique keys (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Make sure we store the PK on this document now that it's saved
        id_field = self._meta["id_field"]
        if created or id_field not in self._meta.get("shard_key", []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        signals.post_save.send(
            self.__class__, document=self, created=created, **signal_kwargs
        )

        self._clear_changed_fields()
        self._created = False

        return self

    async def delete(self, signal_kwargs=None, **write_concern):
        """Delete the :class:`~mongomotor.Document` from the database. This
        will only take effect if the document has been previously saved.

        :parm signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.

        """
        signal_kwargs = signal_kwargs or {}
        signals.pre_delete.send(self.__class__, document=self, **signal_kwargs)

        # Delete FileFields separately
        FileField = _import_class('FileField')
        for name, field in self._fields.items():
            if isinstance(field, FileField):
                getattr(self, name).delete()

        try:
            r = await self._qs.filter(
                **self._object_key).delete(write_concern=write_concern,
                                           _from_doc_delete=True)
            signals.post_delete.send(
                self.__class__, document=self, **signal_kwargs)
        except pymongo.errors.OperationFailure as err:
            message = 'Could not delete document (%s)' % err.message
            raise OperationError(message)

        return r

    async def modify(self, query={}, **update):
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

        updated = await self._qs(**query).modify(new=True, **update)
        if updated is None:
            return False

        self._set_no_deref()
        for field in self._fields_ordered:
            try:
                setattr(self, field, self._reload(field,
                                                  updated[field]))
            except AttributeError:
                setattr(self, field, self._reload(
                    field, updated._data.get(field)))

        self._back_deref()
        self._changed_fields = updated._changed_fields
        self._created = False
        return True

    async def reload(self, *fields, **kwargs):
        """Reloads all attributes from the database.

        :param fields: (optional) args list of fields to reload
        :param max_depth: (optional) depth of dereferencing to follow
        """
        if fields and isinstance(fields[0], int):
            fields = fields[1:]

        if self.pk is None:
            raise self.DoesNotExist("Document does not exist")

        obj = (
            await self._qs.read_preference(ReadPreference.PRIMARY)
            .filter(**self._object_key)
            .only(*fields)
            .limit(1).to_list()
        )

        if obj:
            obj = obj[0]
        else:
            raise self.DoesNotExist("Document does not exist")
        self._set_no_deref()
        for field in obj._data:
            if not fields or field in fields:
                try:
                    setattr(self, field, self._reload(field, obj[field]))
                except (KeyError, AttributeError):
                    try:
                        # If field is a special field, e.g. items is stored as
                        # _reserved_items, a KeyError is thrown. So try to
                        # retrieve the field from _data
                        setattr(self, field, self._reload(
                            field, obj._data.get(field)))
                    except KeyError:
                        # If field is removed from the database while the
                        # object is in memory, a reload would cause a KeyError
                        # i.e. obj.update(unset__field=1) followed by
                        # obj.reload()
                        delattr(self, field)

        self._back_deref()
        self._changed_fields = (
            list(set(self._changed_fields) - set(fields))
            if fields
            else obj._changed_fields
        )
        self._created = False
        return self

    async def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects or
        :class:`~bson.object_id.ObjectId` a maximum depth in order to cut down
        the number queries to mongodb.
        """
        # Make select related work the same for querysets
        max_depth += 1
        queryset = self.clone()
        return await queryset._dereference(queryset, max_depth=max_depth)

    @classmethod
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        if document_cls.__name__.startswith('Patched'):
            return
        return super().register_delete_rule(document_cls, field_name, rule)

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`mongomotor.Document` type from the database.
        """
        cls._collection = None
        db = cls._get_db()
        return db.drop_collection(cls._get_collection_name())

    @classmethod
    async def compare_indexes(cls):
        """Compares the indexes defined in MongoEngine with the ones
        existing in the database. Returns any missing/extra indexes.
        """

        required = cls.list_indexes()

        existing = []
        collection = cls._get_collection()
        for info in (await collection.index_information()).values():
            if "_fts" in info["key"][0]:
                # Useful for text indexes (but not only)
                index_type = info["key"][0][1]
                text_index_fields = info.get("weights").keys()
                existing.append([(key, index_type)
                                for key in text_index_fields])
            else:
                existing.append(info["key"])
        missing = [index for index in required if index not in existing]
        extra = [index for index in existing if index not in required]

        # if { _cls: 1 } is missing, make sure it's *really* necessary
        if [("_cls", 1)] in missing:
            cls_obsolete = False
            for index in existing:
                if includes_cls(index) and index not in extra:
                    cls_obsolete = True
                    break
            if cls_obsolete:
                missing.remove([("_cls", 1)])

        return {"missing": missing, "extra": extra}

    @classmethod
    async def ensure_indexes(cls):
        """Checks the document meta data and ensures all the indexes exist.

        Global defaults can be set in the meta - see
        :doc:`guide/defining-documents`

        By default, this will get called automatically upon first interaction
        with the Document collection (query, save, etc) so unless you disabled
        `auto_create_index`, you shouldn't have to call this manually.

        This also gets called upon every call to Document.save if
        `auto_create_index_on_save` is set to True

        If called multiple times, MongoDB will not re-recreate
        indexes if they exist already

        .. note:: You can disable automatic index creation by setting
                  `auto_create_index` to False in the documents meta data
        """
        background = cls._meta.get("index_background", False)
        index_opts = cls._meta.get("index_opts") or {}
        index_cls = cls._meta.get("index_cls", True)

        collection = cls._get_collection()

        # determine if an index which we are creating includes
        # _cls as its first field; if so, we can avoid creating
        # an extra index on _cls, as mongodb will use the existing
        # index to service queries against _cls
        cls_indexed = False

        # Ensure document-defined indexes are created
        if cls._meta["index_specs"]:
            index_spec = cls._meta["index_specs"]
            for spec in index_spec:
                spec = spec.copy()
                fields = spec.pop("fields")
                cls_indexed = cls_indexed or includes_cls(fields)
                opts = index_opts.copy()
                opts.update(spec)

                # we shouldn't pass 'cls' to the collection.ensureIndex options
                # because of https://jira.mongodb.org/browse/SERVER-769
                if "cls" in opts:
                    del opts["cls"]

                await collection.create_index(
                    fields, background=background, **opts)

        # If _cls is being used (for polymorphism), it needs an index,
        # only if another index doesn't begin with _cls
        if index_cls and not cls_indexed and cls._meta.get(
                "allow_inheritance"):
            # we shouldn't pass 'cls' to the collection.ensureIndex options
            # because of https://jira.mongodb.org/browse/SERVER-769
            if "cls" in index_opts:
                del index_opts["cls"]

            await collection.create_index(
                "_cls", background=background, **index_opts)

    @property
    def _qs(self):
        """
        Returns the queryset to use for updating / reloading / deletions
        """
        if not hasattr(self, '__objects'):
            self.__objects = QuerySet(self, self._get_collection())
        return self.__objects

    async def _save_create(self, doc, force_insert, write_concern):
        """Save a new document.

        Helper method, should only be used inside save().
        """
        collection = self._get_collection()
        with set_write_concern(collection, write_concern) as wc_collection:
            if force_insert:
                r = await wc_collection.insert_one(doc)
                return r.inserted_id
            # insert_one will provoke UniqueError alongside save does not
            # therefore, it need to catch and call replace_one.
            if "_id" in doc:
                select_dict = {"_id": doc["_id"]}
                select_dict = self._integrate_shard_key(doc, select_dict)
                raw_object = wc_collection.find_one_and_replace(
                    select_dict, doc)
                if raw_object:
                    return doc["_id"]

            r = await wc_collection.insert_one(doc)
            object_id = r.inserted_id

        return object_id

    async def _save_update(self, doc, save_condition, write_concern):
        """Update an existing document.

        Helper method, should only be used inside save().
        """
        collection = self._get_collection()
        object_id = doc["_id"]
        created = False

        select_dict = {}
        if save_condition is not None:
            select_dict = transform.query(self.__class__, **save_condition)

        select_dict["_id"] = object_id

        select_dict = self._integrate_shard_key(doc, select_dict)

        update_doc = self._get_update_doc()
        if update_doc:
            upsert = save_condition is None
            with set_write_concern(collection, write_concern) as wc_collection:
                r = await wc_collection.update_one(
                    select_dict, update_doc, upsert=upsert
                )
                last_error = r.raw_result
            if not upsert and last_error["n"] == 0:
                raise SaveConditionError(
                    "Race condition preventing document update detected"
                )
            if last_error is not None:
                updated_existing = last_error.get("updatedExisting")
                if updated_existing is False:
                    created = True
                    # !!! This is bad, means we accidentally created a new,
                    # potentially corrupted document. See
                    # https://github.com/MongoEngine/mongoengine/issues/564

        return object_id, created


class DynamicDocument(Document, DynamicDocumentBase):

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


class EmbeddedDocument(NoDerefInitMixin, EmbeddedDocumentBase):

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


class DynamicEmbeddedDocument(NoDerefInitMixin, DynamicEmbeddedDocumentBase):

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
