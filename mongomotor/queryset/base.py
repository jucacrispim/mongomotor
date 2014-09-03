# -*- coding: utf-8 -*-

import re
import pymongo
from tornado import gen
from bson.code import Code
from mongoengine.fields import ReferenceField
from mongoengine.common import _import_class
from mongoengine.queryset import base
from mongoengine.errors import NotUniqueError
from mongomotor import signals


class BaseQuerySet(base.BaseQuerySet):
    """
    BaseQuerySet that uses motor
    """

    @gen.coroutine
    def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ``ObjectId``\ s
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.

        .. versionadded:: 0.3
        """
        doc_map = {}

        docs = self._collection.find({'_id': {'$in': object_ids}},
                                     **self._cursor_args)
        if self._scalar:
            while (yield docs.fetch_next):
                doc = docs.next_object()
                doc_map[doc['_id']] = self._get_scalar(
                    self._document._from_son(doc))
        elif self._as_pymongo:
            while (yield docs.fetch_next):
                doc = docs.next_object()
                doc_map[doc['_id']] = self._get_as_pymongo(doc)
        else:
            while (yield docs.fetch_next):
                doc = docs.next_object()
                doc_map[doc['_id']] = self._document._from_son(doc)

        return doc_map

    @gen.coroutine
    def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~mongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.

        .. versionadded:: 0.3
        """
        queryset = self.clone()
        queryset = yield queryset.limit(2)
        queryset = queryset.filter(*q_objs, **query)

        try:
            result = yield next(queryset)
        except StopIteration:
            msg = ("%s matching query does not exist."
                   % queryset._document._class_name)
            raise queryset._document.DoesNotExist(msg)

        if not result:
            msg = ("%s matching query does not exist."
                   % queryset._document._class_name)
            raise queryset._document.DoesNotExist(msg)


        try:
            n = yield next(queryset)
        except StopIteration:
            return result

        if not n:
            return result

        yield queryset.rewind()
        message = '%d items returned, instead of 1' % (yield queryset.count())
        raise queryset._document.MultipleObjectsReturned(message)

    @gen.coroutine
    def rewind(self):
        """Rewind the cursor to its unevaluated state.

        .. versionadded:: 0.3
        """
        self._iter = False
        cursor = yield self._cursor
        cursor.rewind()

    @gen.coroutine
    def delete(self, write_concern=None, _from_doc_delete=False):
        """Delete the documents matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param _from_doc_delete: True when called from document delete therefore
            signals will have been triggered so don't loop.
        """
        queryset = self.clone()
        doc = queryset._document

        if write_concern is None:
            write_concern = {}

        # Handle deletes where skips or limits have been applied or
        # there is an untriggered delete signal
        has_delete_signal = signals.signals_available and (
            signals.pre_delete.has_receivers_for(self._document) or
            signals.post_delete.has_receivers_for(self._document))

        call_document_delete = (queryset._skip or queryset._limit or
                                has_delete_signal) and not _from_doc_delete

        if call_document_delete:
            for d in queryset:
                doc = yield d
                yield doc.delete(write_concern=write_concern)
            return

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == base.DENY and (yield document_cls.objects(
                    **{field_name + '__in': self}).count() > 0):
                msg = ("Could not delete document (%s.%s refers to it)"
                       % (document_cls.__name__, field_name))
                raise OperationError(msg)

        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == base.CASCADE:
                ref_q = document_cls.objects(**{field_name + '__in': self})
                ref_q_count = yield ref_q.count()
                if (doc != document_cls and ref_q_count > 0
                   or (doc == document_cls and ref_q_count > 0)):
                    yield ref_q.delete(write_concern=write_concern)

            # Need to work on .update
            elif rule == base.NULLIFY:
                document_cls.objects(**{field_name + '__in': self}).update(
                    write_concern=write_concern, **{'unset__%s' % field_name: 1})
            elif rule == base.PULL:
                document_cls.objects(**{field_name + '__in': self}).update(
                    write_concern=write_concern,
                    **{'pull_all__%s' % field_name: self})

        yield queryset._collection.remove((yield queryset._query),
                                          write_concern=write_concern)

    @gen.coroutine
    def insert(self, doc_or_docs, load_bulk=True, write_concern=None):
        """bulk insert documents

        :param docs_or_doc: a document or list of documents to be inserted
        :param load_bulk (optional): If True returns the list of document
            instances
        :param write_concern: Extra keyword arguments are passed down to
                :meth:`~pymongo.collection.Collection.insert`
                which will be used as options for the resultant
                ``getLastError`` command.  For example,
                ``insert(..., {w: 2, fsync: True})`` will wait until at least
                two servers have recorded the write and will force an fsync on
                each server being written to.

        By default returns document instances, set ``load_bulk`` to False to
        return just ``ObjectIds``

        .. versionadded:: 0.5
        """
        Document = _import_class('Document')

        if write_concern is None:
            write_concern = {}

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, Document) or issubclass(docs.__class__, Document):
            return_one = True
            docs = [docs]

        raw = []
        for doc in docs:
            if not isinstance(doc, self._document):
                msg = ("Some documents inserted aren't instances of %s"
                       % str(self._document))
                raise OperationError(msg)
            if doc.pk and not doc._created:
                msg = "Some documents have ObjectIds use doc.update() instead"
                raise OperationError(msg)
            raw.append(doc.to_mongo())

        yield signals.pre_bulk_insert.send(self._document, documents=docs)
        try:
            ids = yield self._collection.insert(raw, **write_concern)
        except pymongo.errors.OperationFailure as err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = 'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % str(err))
            raise OperationError(message % str(err))

        if not load_bulk:
            yield signals.post_bulk_insert.send(
                self._document, documents=docs, loaded=False)
            return return_one and ids[0] or ids

        documents = yield self.in_bulk(ids)
        results = []
        for obj_id in ids:
            results.append(documents.get(obj_id))
        yield signals.post_bulk_insert.send(
            self._document, documents=results, loaded=True)
        return return_one and results[0] or results

    @gen.coroutine
    def limit(self, n):
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).

        :param n: the maximum number of objects to return
        """
        queryset = self.clone()
        cursor = yield queryset._cursor
        if n == 0:
            cursor.limit(1)
        else:
            cursor.limit(n)
        queryset._limit = n
        # Return self to allow chaining
        return queryset

    @gen.coroutine
    def __next__(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        """
        if not hasattr(self, '_next_doc'):
            self._next_doc = yield self._get_next_doc()

        raw_doc = self._next_doc

        if self._limit == 0 or self._none or not raw_doc:
            raise StopIteration

        if self._as_pymongo:
            doc = self._get_as_pymongo(raw_doc)
            self._next_doc = yield self._get_next_doc()
            return doc

        doc = self._document._from_son(raw_doc,
                                       _auto_dereference=self._auto_dereference)
        if self._scalar:
            doc = self._get_scalar(doc)
            self._next_doc = yield self._get_next_doc()
            return doc

        self._next_doc = yield self._get_next_doc()
        return doc

    @gen.coroutine
    def __getitem__(self, key):
        """Support skip and limit using getitem and slicing syntax.
        """
        queryset = self.clone()
        # Slice provided
        if isinstance(key, slice):
            try:
                cursor = yield queryset._cursor
                queryset._cursor_obj = cursor[key]
                queryset._skip, queryset._limit = key.start, key.stop
                if key.start and key.stop:
                    queryset._limit = key.stop - key.start
            except IndexError as err:
                # PyMongo raises an error if key.start == key.stop, catch it,
                # bin it, kill it.
                start = key.start or 0
                if start >= 0 and key.stop >= 0 and key.step is None:
                    if start == key.stop:
                        queryset.limit(0)
                        queryset._skip = key.start
                        queryset._limit = key.stop - start
                        return queryset
                raise err
            # Allow further QuerySet modifications to be performed
            return queryset
        # Integer index provided
        elif isinstance(key, int):
            new_cursor = yield queryset._cursor
            new_cursor = new_cursor[key]
            yield new_cursor.fetch_next
            raw_doc = new_cursor.next_object()
            if queryset._scalar:
                return queryset._get_scalar(
                    queryset._document._from_son(
                        raw_doc,
                        _auto_dereference=self._auto_dereference))
            if queryset._as_pymongo:
                n = yield next(queryset._cursor)
                return queryset._get_as_pymongo(n)
            return queryset._document._from_son(
                raw_doc,
                _auto_dereference=self._auto_dereference)
        raise AttributeError


    @gen.coroutine
    def  _get_next_doc(self):
        cursor = yield self._cursor
        yield cursor.fetch_next
        n = cursor.next_object()
        return  n

    @property
    @gen.coroutine
    def _cursor(self):
        if self._cursor_obj is None:
            self._cursor_obj = self._collection.find((yield self._query),
                                                     **self._cursor_args)
            # Apply where clauses to cursor
            if self._where_clause:
                where_clause = self._sub_js_fields(self._where_clause)
                self._cursor_obj.where(where_clause)

            if self._ordering:
                # Apply query ordering
                self._cursor_obj.sort(self._ordering)
            elif self._document._meta['ordering']:
                # Otherwise, apply the ordering from the document model
                order = self._get_order_by(self._document._meta['ordering'])
                self._cursor_obj.sort(order)

            if self._limit is not None:
                self._cursor_obj.limit(self._limit)

            if self._skip is not None:
                self._cursor_obj.skip(self._skip)

            if self._hint != -1:
                self._cursor_obj.hint(self._hint)

        return self._cursor_obj

    @property
    @gen.coroutine
    def _query(self):
        if self._mongo_query is None:
            self._mongo_query = yield self._query_obj.to_query(self._document)
            if self._class_check:
                self._mongo_query.update(self._initial_query)
        return self._mongo_query


    @gen.coroutine
    def count(self, with_limit_and_skip=True):
        """Count the selected elements in the query.

        :param with_limit_and_skip (optional): take any :meth:`limit` or
        :meth:`skip` that has been applied to this cursor into account when
        getting the count
        """
        if self._limit == 0 and with_limit_and_skip or self._none:
            return 0
        cursor = yield self._cursor
        n = yield cursor.count(with_limit_and_skip=with_limit_and_skip)
        return n
