# -*- coding: utf-8 -*-

import re
import pymongo
from pymongo.read_preferences import ReadPreference
from bson.dbref import DBRef
import tornado
from tornado import gen
from mongoengine.queryset import OperationError, NotUniqueError
from mongoengine import (Document as DocumentBase,
                         EmbeddedDocument as EmbeddedDocumentBase,
                         DynamicDocument as DynamicDocumentBase)
from mongoengine.base.metaclasses import (TopLevelDocumentMetaclass,
                                          DocumentMetaclass)
from mongoengine.document import _import_class
from tornado import gen
from mongomotor import signals
from mongomotor.base.document import BaseDocumentMotor


class Document(BaseDocumentMotor, DocumentBase,
               metaclass=TopLevelDocumentMetaclass):
    """
    Document version that uses motor mongodb driver.
    It's a copy of some mongoengine.Document methods
    that uses tornado.gen.coroutine and yield things
    to use motor with generator style.
    """

    my_metaclass = TopLevelDocumentMetaclass

    @gen.coroutine
    def delete(self, **write_concern):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        """
        signals.pre_delete.send(self.__class__, document=self)
        try:
            qs = self._qs.filter(**self._object_key)
            yield qs.delete(write_concern=write_concern, _from_doc_delete=True)
        except pymongo.errors.OperationFailure as err:
            message = 'Could not delete document (%s)' % err.message
            raise OperationError(message)
        signals.post_delete.send(self.__class__, document=self)

    @gen.coroutine
    def save(self, force_insert=False, validate=True, clean=True,
             write_concern=None,  cascade=None, cascade_kwargs=None,
             _refs=None, **kwargs):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
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

        """
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            yield self.validate(clean=clean)

        if write_concern is None:
            write_concern = {"w": 1}

        doc = self.to_mongo()

        created = ('_id' not in doc or self._created or force_insert)

        signals.pre_save_post_validation.send(self.__class__, document=self,
                                              created=created)

        try:
            collection = self._get_collection()
            if created:
                if force_insert:
                    # I realy don't know why this stupid line isn't covered. Maybe later
                    # I take a look at it.
                    object_id = yield collection.insert(doc, **write_concern) # pragma: no cover
                else:
                    object_id = yield collection.save(doc, **write_concern)
            else:
                object_id = doc['_id']
                updates, removals = self._delta()
                # Need to add shard key to query, or you get an error
                select_dict = {'_id': object_id}
                shard_key = self.__class__._meta.get('shard_key', tuple())
                for k in shard_key:
                    actual_key = self._db_field_map.get(k, k)
                    select_dict[actual_key] = doc[actual_key]

                def is_new_object(last_error):
                    if last_error is not None:
                        updated = last_error.get("updatedExisting")
                        if updated is not None:
                            return not updated
                    return created  # pragma: no cover

                update_query = {}

                if updates:
                    update_query["$set"] = updates
                if removals:
                    update_query["$unset"] = removals  # pragma: no cover
                if updates or removals:
                    last_error = yield collection.update(select_dict,
                                                         update_query,
                                                         upsert=True,
                                                         **write_concern)
                    created = is_new_object(last_error)

            if cascade is None:
                cascade = self._meta.get('cascade', False) or \
                          cascade_kwargs is not None

            if cascade:
                kwargs = {
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs['_refs'] = _refs
                yield self.cascade_save(**kwargs)

        except pymongo.errors.OperationFailure as err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = 'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % str(err))
            raise OperationError(message % str(err))  # pragma: no cover
        id_field = self._meta['id_field']
        if id_field not in self._meta.get('shard_key', []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        self._clear_changed_fields()
        self._created = False
        signals.post_save.send(self.__class__, document=self, created=created)

        return self

    @gen.coroutine
    def cascade_save(self, *args, **kwargs):
        """Recursively saves any references /
           generic references on an objects"""
        _refs = kwargs.get('_refs', []) or []

        ReferenceField = _import_class('ReferenceField')
        GenericReferenceField = _import_class('GenericReferenceField')

        for name, cls in list(self._fields.items()):
            if not isinstance(cls, (ReferenceField,
                                    GenericReferenceField)):
                continue

            ref = self._data.get(name)
            if not ref or isinstance(ref, DBRef):
                continue  # pragma: no cover

            if not getattr(ref, '_changed_fields', True):
                continue  # pragma: no cover

            ref_id = "%s,%s" % (ref.__class__.__name__, str(ref._data))
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                yield ref.save(**kwargs)
                ref._changed_fields = []


    @classmethod
    @gen.coroutine
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        cls._collection = None
        db = cls._get_db()
        yield db.drop_collection(cls._get_collection_name())

    @classmethod
    @gen.coroutine
    def compare_indexes(cls):
        """ Compares the indexes defined in MongoEngine with the ones existing
        in the database. Returns any missing/extra indexes.
        """
        required = cls.list_indexes()
        existing = [
            info['key'] for info in
            list((yield cls._get_collection().index_information()).values())]

        missing = [index for index in required if index not in existing]
        extra = [index for index in existing if index not in required]

        # if { _cls: 1 } is missing, make sure it's *really* necessary
        if [('_cls', 1)] in missing:
            cls_obsolete = False
            for index in existing:
                if includes_cls(index) and index not in extra:
                    cls_obsolete = True
                    break
            if cls_obsolete:
                missing.remove([('_cls', 1)])

        return {'missing': missing, 'extra': extra}

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
                #obj._changed_fields.pop(obj._changed_fields.index(field))
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
                      metaclass=TopLevelDocumentMetaclass):

    my_metaclass  = TopLevelDocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        DynamicDocumentBase.__delattr__(self, *args, **kwargs)


class EmbeddedDocument(BaseDocumentMotor, EmbeddedDocumentBase,
                       metaclass=DocumentMetaclass):

    my_metaclass = TopLevelDocumentMetaclass

    def __init__(self, *args, **kwargs):
        super(EmbeddedDocument, self).__init__(*args, **kwargs)
        self._instance = None
        self._changed_fields = []
