# -*- coding: utf-8 -*-

import re
import pymongo
from bson.dbref import DBRef
from tornado import gen
from mongoengine.queryset import OperationError, NotUniqueError
from mongoengine import Document as DocumentBase
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass
from mongoengine.document import _import_class
from tornado import gen
from mongomotor import signals


class Document(DocumentBase, metaclass=TopLevelDocumentMetaclass):
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

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Added cascading saves
        .. versionchanged:: 0.8
            Cascade saves are optional and default to False.  If you want
            fine grain control then you can turn off using document
            meta['cascade'] = True.  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values.
        """
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate(clean=clean)

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
