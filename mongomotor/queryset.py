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

from bson.code import Code
from bson import SON
import functools
import os
from mongoengine import signals, DENY, CASCADE, NULLIFY, PULL
from mongoengine.connection import get_db
from mongoengine.queryset.queryset import QuerySet as MEQuerySet
from mongoengine.errors import OperationError
from motor.core import coroutine_annotation
from mongomotor.exceptions import ConfusionError
from mongomotor.metaprogramming import (get_future, AsyncGenericMetaclass,
                                        Async, asynchronize)
from mongomotor.monkey import MonkeyPatcher

# for tests
TEST_ENV = os.environ.get('MONGOMOTOR_TEST_ENV')


class QuerySet(MEQuerySet, metaclass=AsyncGenericMetaclass):

    distinct = Async()
    explain = Async()
    in_bulk = Async()
    map_reduce = Async()
    modify = Async()
    update = Async()

    def __repr__(self):  # pragma no cover
        return self.__class__.__name__

    def __len__(self):
        raise TypeError('len() is not supported. Use count()')

    def _iter_results(self):
        try:
            return super()._iter_results()
        except StopIteration:
            raise StopAsyncIteration

    def __getitem__(self, index):
        # If we received an slice we will return a queryset
        # and as we will not touch the db now we do not need a future
        # here
        if isinstance(index, slice):
            return super().__getitem__(index)

        else:
            sync_getitem = MEQuerySet.__getitem__
            async_getitem = asynchronize(sync_getitem)
            return async_getitem(self, index)

    def __aiter__(self):
        return self

    async def __anext__(self):
        async for doc in self._cursor:
            mm_doc = self._document._from_son(
                doc,
                _auto_dereference=self._auto_dereference)
            return mm_doc
        else:
            raise StopAsyncIteration()

    @coroutine_annotation
    def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~mongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.
        """

        queryset = self.clone()
        queryset = queryset.order_by().limit(2)
        queryset = queryset.filter(*q_objs, **query)

        future = get_future(self)

        def _get_cb(done_future):
            docs = done_future.result()
            if len(docs) < 1:
                msg = ("%s matching query does not exist."
                       % queryset._document._class_name)
                future.set_exception(queryset._document.DoesNotExist(msg))

            elif len(docs) > 1:
                msg = 'More than 1 item returned'
                future.set_exception(
                    queryset._document.MultipleObjectsReturned(msg))
            else:
                future.set_result(docs[0])

        list_future = queryset.to_list(length=2)
        list_future.add_done_callback(_get_cb)  # pragma no cover
        return future

    @coroutine_annotation
    def first(self):
        """Retrieve the first object matching the query.
        """
        queryset = self.clone()
        first_future = queryset[0]
        future = get_future(self)

        def first_cb(first_future):
            try:
                result = first_future.result()
                future.set_result(result)
            except IndexError:
                result = None
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

        first_future.add_done_callback(first_cb)
        return future

    @coroutine_annotation
    def count(self, with_limit_and_skip=True):
        """Counts the documents in the queryset.

        :param with_limit_and_skip: Indicates if limit and skip applied to
          the queryset should be taken into account."""

        if self._limit == 0 and with_limit_and_skip or self._none:
            return 0

        kw = {}
        if with_limit_and_skip and self._limit:
            kw['limit'] = self._limit

        if with_limit_and_skip and self._skip:
            kw['skip'] = self._skip

        return self._collection.count_documents(self._query, **kw)

    @coroutine_annotation
    def insert(self, doc_or_docs, load_bulk=True, write_concern=None):
        """bulk insert documents

        :param doc_or_docs: a document or list of documents to be inserted
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
        """

        super_insert = MEQuerySet.insert
        async_in_bulk = self.in_bulk
        # this sync method is not really sync, it uses motor sockets and
        # greenlets events, but looks like sync, so...
        sync_in_bulk = functools.partial(self.in_bulk.__wrapped__, self)
        insert_future = get_future(self)

        with MonkeyPatcher() as patcher:
            # here we change the method with the async api for the method
            # with a sync api so I don't need to rewrite the mongoengine
            # method.
            patcher.patch_item(self, 'in_bulk', sync_in_bulk, undo=False)
            future = asynchronize(super_insert)(self, doc_or_docs,
                                                load_bulk=load_bulk,
                                                write_concern=write_concern)

            def cb(future):
                try:
                    result = future.result()
                    insert_future.set_result(result)
                except Exception as e:
                    insert_future.set_exception(e)
                finally:
                    patcher.patch_item(self, 'in_bulk', async_in_bulk,
                                       undo=False)

            future.add_done_callback(cb)

        return insert_future

    async def delete(self, write_concern=None, _from_doc_delete=False,
                     cascade_refs=None):
        """Deletes the documents matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param _from_doc_delete: True when called from document delete
          therefore signals will have been triggered so don't loop.

        :returns number of deleted documents
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
            async_method = asynchronize(self._document_delete)
            return async_method(queryset, write_concern)

        await self._check_delete_rules(doc, queryset, cascade_refs,
                                       write_concern)

        r = await queryset._collection.delete_many(
            queryset._query, **write_concern)

        return r

    @coroutine_annotation
    def upsert_one(self, write_concern=None, **update):
        """Overwrite or add the first document matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param update: Django-style update keyword arguments

        :returns the new or overwritten document

        """

        update_future = self.update(multi=False, upsert=True,
                                    write_concern=write_concern,
                                    full_result=True, **update)

        upsert_future = get_future(self)

        def update_cb(update_future):
            try:
                result = update_future.result().raw_result
                if result['updatedExisting']:
                    document_future = self.first()
                else:
                    document_future = self._document.objects.with_id(
                        result['upserted'])

                def doc_cb(document_future):
                    try:
                        result = document_future.result()
                        upsert_future.set_result(result)
                    except Exception as e:
                        upsert_future.set_exception(e)

                document_future.add_done_callback(doc_cb)
            except Exception as e:
                upsert_future.set_exception(e)

        update_future.add_done_callback(update_cb)
        return upsert_future

    @coroutine_annotation
    def to_list(self, length=100):
        """Returns a list of the current documents in the queryset.

        :param length: maximum number of documents to return for this call."""

        list_future = get_future(self)

        def _to_list_cb(future):
            # Transforms mongo's raw documents into
            # mongomotor documents
            docs_list = future.result()
            final_list = [self._document._from_son(
                d, _auto_dereference=self._auto_dereference)
                for d in docs_list]

            list_future.set_result(final_list)

        cursor = self._cursor
        future = cursor.to_list(length)
        future.add_done_callback(_to_list_cb)
        return list_future

    async def item_frequencies(self, field, normalize=False):
        """Returns a dictionary of all items present in a field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds, or searching documents.

        .. note::

            Can only do direct simple mappings and cannot map across
            :class:`~mongoengine.fields.ReferenceField` or
            :class:`~mongoengine.fields.GenericReferenceField` for more complex
            counting a manual aggretation call would be required.

        If the field is a :class:`~mongoengine.fields.ListField`,
        the items within each list will be counted individually.

        :param field: the field to use
        :param normalize: normalize the results so they add to 1.0
        """

        cursor = self._document._get_collection().aggregate([
            {'$match': self._query},
            {'$unwind': f'${field}'},
            {'$group': {'_id': '$' + field, 'total': {'$sum': 1}}}
        ])
        freqs = {}
        async for doc in cursor:
            freqs[doc['_id']] = doc['total']

        if normalize:
            count = sum(freqs.values())
            freqs = dict([(k, float(v) / count)
                          for k, v in list(freqs.items())])

        return freqs

    async def average(self, field):
        """Average over the values of the specified field.

        :param field: the field to average over; use dot-notation to refer to
            embedded document fields

        This method is more performant than the regular `average`, because it
        uses the aggregation framework instead of map-reduce.
        """
        cursor = self._document._get_collection().aggregate([
            {'$match': self._query},
            {'$group': {'_id': 'avg', 'total': {'$avg': '$' + field}}}
        ])

        avg = 0
        async for doc in cursor:
            avg = doc['total']
            break

        return avg

    async def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot-notation to refer to
            embedded document fields

        This method is more performant than the regular `sum`, because it uses
        the aggregation framework instead of map-reduce.
        """
        cursor = self._document._get_collection().aggregate([
            {'$match': self._query},
            {'$group': {'_id': 'sum', 'total': {'$sum': '$' + field}}}
        ])

        r = 0
        async for doc in cursor:
            r = doc['total']
            break

        return r

    @property
    @coroutine_annotation
    def fetch_next(self):
        return self._cursor.fetch_next

    def next_object(self):
        raw = self._cursor.next_object()
        return self._document._from_son(
            raw, _auto_dereference=self._auto_dereference)

    def no_cache(self):
        """Convert to a non-caching queryset
        """
        if self._result_cache is not None:
            raise OperationError('QuerySet already cached')

        return self._clone_into(QuerySetNoCache(self._document,
                                                self._collection))

    def _get_code(self, func):
        f_scope = {}
        if isinstance(func, Code):
            f_scope = func.scope
            func = str(func)
        func = Code(self._sub_js_fields(func), f_scope)
        return func

    def _get_output(self, output):

        if isinstance(output, str) or isinstance(output, SON):
            out = output

        elif isinstance(output, dict):
            ordered_output = []
            for part in ('replace', 'merge', 'reduce'):
                value = output.get(part)
                if value:
                    ordered_output.append((part, value))
                    break

            else:
                raise OperationError("actionData not specified for output")

            db_alias = output.get('db_alias')
            remaing_args = ['db', 'sharded', 'nonAtomic']

            if db_alias:
                ordered_output.append(('db', get_db(db_alias).name))
                del remaing_args[0]

            for part in remaing_args:
                value = output.get(part)
                if value:
                    ordered_output.append((part, value))

            out = SON(ordered_output)

        else:
            raise ConfusionError('Bad output type {}'.format(type(output)))

        return out

    async def _check_delete_rules(self, doc, queryset, cascade_refs,
                                  write_concern):
        """Checks the delete rules for documents being deleted in a queryset.
        Raises an exception if any document has a DENY rule."""

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
        delete_rules = delete_rules.copy()
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get('abstract'):
                continue
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == DENY and document_cls.objects(
                    **{field_name + '__in': self}).count() > 0:
                msg = ("Could not delete document (%s.%s refers to it)"
                       % (document_cls.__name__, field_name))
                raise OperationError(msg)

        if not delete_rules:
            return

        r = None
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get('abstract'):
                continue
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == CASCADE:
                cascade_refs = set() if cascade_refs is None else cascade_refs
                for ref in queryset:
                    cascade_refs.add(ref.id)
                ref_q = document_cls.objects(**{field_name + '__in': self,
                                                'id__nin': cascade_refs})

                count = await ref_q.count()
                if count > 0:
                    r = await ref_q.delete(write_concern=write_concern,
                                           cascade_refs=cascade_refs)

            elif rule in (NULLIFY, PULL):
                if rule == NULLIFY:
                    updatekw = {'unset__%s' % field_name: 1}
                else:
                    updatekw = {'pull_all__%s' % field_name: self}

                r = await document_cls.objects(
                    **{field_name + '__in': self}).update(
                        write_concern=write_concern, **updatekw)

        return r

    def _document_delete(self, queryset, write_concern):
        """Delete the documents in queryset by calling the document's delete
        method."""

        cnt = 0
        for doc in queryset:
            doc.delete(**write_concern)
            cnt += 1
        return cnt

    def _get_loop(self):
        """Returns the ioloop for this queryset."""

        db = self._document._get_db()
        loop = db.get_io_loop()
        return loop


class QuerySetNoCache(QuerySet):
    """A non caching QuerySet"""

    def cache(self):
        """Convert to a caching queryset
        """
        return self._clone_into(QuerySet(self._document, self._collection))

    def __iter__(self):
        queryset = self
        if queryset._iter:
            queryset = self.clone()
        queryset.rewind()
        return queryset
