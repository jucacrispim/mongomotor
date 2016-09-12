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
import textwrap
from mongoengine import signals, DENY, CASCADE, NULLIFY, PULL
from mongoengine.connection import get_db
from mongoengine.document import MapReduceDocument
from mongoengine.queryset.queryset import QuerySet as BaseQuerySet
from mongoengine.errors import OperationError
from motor.core import coroutine_annotation
from mongomotor import PY35
from mongomotor.exceptions import ConfusionError
from mongomotor.metaprogramming import (get_future, AsyncGenericMetaclass,
                                        Async, asynchronize)
from mongomotor.monkey import MonkeyPatcher


class QuerySet(BaseQuerySet, metaclass=AsyncGenericMetaclass):

    distinct = Async()
    explain = Async()
    in_bulk = Async()
    map_reduce = Async()
    modify = Async()
    update = Async()

    def __repr__(self):
        return self.__class__.__name__

    def __len__(self):
        raise TypeError('len() is not supported. Use count()')

    def __getitem__(self, index):
        # It we received an slice we will return a queryset
        # and as we will not touch the db now wd do not need a future
        # here
        if isinstance(index, slice):
            return super().__getitem__(index)

        else:
            sync_getitem = BaseQuerySet.__getitem__
            async_getitem = asynchronize(sync_getitem)
            return async_getitem(self, index)

    if PY35:
        exec(textwrap.dedent("""
        from mongomotor.decorators import aiter_compat
        @aiter_compat
        def __aiter__(self):
            return self

        async def __anext__(self):
            async for doc in self._cursor:
                mm_doc = self._document._from_son(
                    doc, only_fields=self.only_fields,
                    _auto_dereference=self._auto_dereference)
                return mm_doc
            else:
                raise StopAsyncIteration()
        """), globals(), locals())

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
        list_future.add_done_callback(_get_cb)
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

        return super().count(with_limit_and_skip)

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

        super_insert = BaseQuerySet.insert
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

    @coroutine_annotation
    def delete(self, write_concern=None, _from_doc_delete=False,
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

        dr_future = self._check_delete_rules(doc, queryset, cascade_refs,
                                             write_concern)

        ret_future = get_future(self)

        def dr_cb(dr_future):
            """callback for _check_delete_rules future"""
            try:
                dr_future.result()

                remove_future = queryset._collection.remove(
                    queryset._query, **write_concern)

                def r_cb(remove_future):
                    """Callback for _collection.remove"""

                    try:
                        result = remove_future.result()
                        if result:
                            ret_future.set_result(result.get('n'))
                    except Exception as e:
                        ret_future.set_exception(e)

                remove_future.add_done_callback(r_cb)

            except Exception as e:
                ret_future.set_exception(e)

        dr_future.add_done_callback(dr_cb)
        return ret_future

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
                result = update_future.result()
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
                d, _auto_dereference=self._auto_dereference,
                only_fields=self.only_fields) for d in docs_list]

            list_future.set_result(final_list)

        cursor = self._cursor
        future = cursor.to_list(length)
        future.add_done_callback(_to_list_cb)
        return list_future

    @coroutine_annotation
    def map_reduce(self, map_f, reduce_f, output, **mr_kwargs):
        """Perform a map/reduce query using the current query spec
        and ordering. While ``map_reduce`` respects ``QuerySet`` chaining,
        it must be the last call made, as it does not return a maleable
        ``QuerySet``.

        :param map_f: map function, as :class:`~bson.code.Code` or string
        :param reduce_f: reduce function, as
                         :class:`~bson.code.Code` or string
        :param output: output collection name, if set to 'inline' will try to
           use :class:`~pymongo.collection.Collection.inline_map_reduce`
           This can also be a dictionary containing output options.

        :param mr_kwargs: Arguments for mongodb map_reduce
           see: https://docs.mongodb.com/manual/reference/command/mapReduce/
           for more information

        Returns a dict with the full response of the server

        .. note::

            This is different from mongoengine's map_reduce. It does not
            support inline map reduce, for that use
            :meth:`~mongomotor.queryset.QuerySet.inline_map_reduce`. And
            It does not return a generator with MapReduceDocument, but
            returns the server response instead.
        """

        if output == 'inline':
            raise OperationError(
                'For inline output please use inline_map_reduce')

        queryset = self.clone()

        map_f = self._get_code(map_f)
        reduce_f = self._get_code(reduce_f)

        mr_kwargs.update({'query': queryset._query})

        if mr_kwargs.get('finalize'):
            mr_kwargs['finalize'] = self._get_code(mr_kwargs['finalize'])

        mr_kwargs['out'] = self._get_output(output)
        mr_kwargs['full_response'] = True

        return queryset._collection.map_reduce(map_f, reduce_f, **mr_kwargs)

    @coroutine_annotation
    def inline_map_reduce(self, map_f, reduce_f, **mr_kwargs):
        """Perform a map/reduce query using the current query spec
        and ordering. While ``map_reduce`` respects ``QuerySet`` chaining,
        it must be the last call made, as it does not return a maleable
        ``QuerySet``.

        :param map_f: map function, as :class:`~bson.code.Code` or string
        :param reduce_f: reduce function, as
                         :class:`~bson.code.Code` or string

        :param mr_kwargs: Arguments for mongodb map_reduce
           see: https://docs.mongodb.com/manual/reference/command/mapReduce/
           for more information

        Returns a generator of MapReduceDocument with the map/reduce results.

        .. note::

           This method only works with inline map/reduce. If you want to
           send the output to a collection use
           :meth:`~mongomotor.queryset.Queryset.map_reduce`.
        """

        queryset = self.clone()

        if mr_kwargs.get('out') and mr_kwargs.get('out') != 'inline':
            msg = 'inline_map_reduce only supports inline output. '
            msg += 'To send the result to a collection use map_reduce'
            raise OperationError(msg)

        map_f = self._get_code(map_f)
        reduce_f = self._get_code(reduce_f)

        mr_kwargs.update({'query': queryset._query})

        if mr_kwargs.get('finalize'):
            mr_kwargs['finalize'] = self._get_code(mr_kwargs['finalize'])

        mr_future = queryset._collection.inline_map_reduce(
            map_f, reduce_f, **mr_kwargs)
        future = get_future(self)

        def inline_mr_cb(result_future):
            result = result_future.result()
            gen = (MapReduceDocument(queryset._document, queryset._collection,
                                     doc['_id'], doc['value'])
                   for doc in result)
            future.set_result(gen)

        mr_future.add_done_callback(inline_mr_cb)
        return future

    @coroutine_annotation
    def item_frequencies(self, field, normalize=False):
        """Returns a dictionary of all items present in a field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds, or searching documents.

        .. note::

            Can only do direct simple mappings and cannot map across
            :class:`~mongoengine.fields.ReferenceField` or
            :class:`~mongoengine.fields.GenericReferenceField` for more complex
            counting a manual map reduce call would is required.

        If the field is a :class:`~mongoengine.fields.ListField`,
        the items within each list will be counted individually.

        :param field: the field to use
        :param normalize: normalize the results so they add to 1.0
        :param map_reduce: Use map_reduce over exec_js
        """

        map_func = """
            function() {
                var path = '{{~%(field)s}}'.split('.');
                var field = this;

                for (p in path) {
                    if (typeof field != 'undefined')
                       field = field[path[p]];
                    else
                       break;
                }
                if (field && field.constructor == Array) {
                    field.forEach(function(item) {
                        emit(item, 1);
                    });
                } else if (typeof field != 'undefined') {
                    emit(field, 1);
                } else {
                    emit(null, 1);
                }
            }
        """ % dict(field=field)
        reduce_func = """
            function(key, values) {
                var total = 0;
                var valuesSize = values.length;
                for (var i=0; i < valuesSize; i++) {
                    total += parseInt(values[i], 10);
                }
                return total;
            }
        """
        mr_future = self.inline_map_reduce(map_func, reduce_func)
        future = get_future(self)

        def item_frequencies_cb(mr_future):
            values = mr_future.result()
            frequencies = {}
            for f in values:
                key = f.key
                if isinstance(key, float):
                    if int(key) == key:
                        key = int(key)
                frequencies[key] = int(f.value)

            if normalize:
                count = sum(frequencies.values())
                frequencies = dict([(k, float(v) / count)
                                    for k, v in list(frequencies.items())])

            future.set_result(frequencies)

        mr_future.add_done_callback(item_frequencies_cb)
        return future

    @coroutine_annotation
    def average(self, field):
        """Average over the values of the specified field.

        :param field: the field to average over; use dot-notation to refer to
            embedded document fields
        """
        map_func = """
            function() {
                var path = '{{~%(field)s}}'.split('.'),
                field = this;

                for (p in path) {
                    if (typeof field != 'undefined')
                       field = field[path[p]];
                    else
                       break;
                }

                if (field && field.constructor == Array) {
                    field.forEach(function(item) {
                        emit(1, {t: item||0, c: 1});
                    });
                } else if (typeof field != 'undefined') {
                    emit(1, {t: field||0, c: 1});
                }
            }
        """ % dict(field=field)

        reduce_func = Code("""
            function(key, values) {
                var out = {t: 0, c: 0};
                for (var i in values) {
                    var value = values[i];
                    out.t += value.t;
                    out.c += value.c;
                }
                return out;
            }
        """)

        finalize_func = Code("""
            function(key, value) {
                return value.t / value.c;
            }
        """)

        future = get_future(self)
        mr_future = self.inline_map_reduce(map_func, reduce_func,
                                           finalize=finalize_func)

        def average_cb(mr_future):
            results = mr_future.result()
            for result in results:
                average = result.value
                break
            else:
                average = 0

            future.set_result(average)

        mr_future.add_done_callback(average_cb)
        return future

    @coroutine_annotation
    def aggregate_average(self, field):
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

        fn_future = cursor.fetch_next
        future = get_future(self)

        def fetch_next_cb(fn_future):
            result = fn_future.result()
            if result:
                doc = cursor.next_object()
                avg = doc['total']
            else:
                avg = 0

            future.set_result(avg)

        fn_future.add_done_callback(fetch_next_cb)
        return future

    @coroutine_annotation
    def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot-notation to refer to
            embedded document fields
        """
        map_func = """
            function() {
                var path = '{{~%(field)s}}'.split('.'),
                field = this;

                for (p in path) {
                    if (typeof field != 'undefined')
                       field = field[path[p]];
                    else
                       break;
                }

                if (field && field.constructor == Array) {
                    field.forEach(function(item) {
                        emit(1, item||0);
                    });
                } else if (typeof field != 'undefined') {
                    emit(1, field||0);
                }
            }
        """ % dict(field=field)

        reduce_func = Code("""
            function(key, values) {
                var sum = 0;
                for (var i in values) {
                    sum += values[i];
                }
                return sum;
            }
        """)

        mr_future = self.inline_map_reduce(map_func, reduce_func)
        future = get_future(self)

        def sum_cb(mr_future):
            results = mr_future.result()

            for result in results:
                r = result.value
                break
            else:
                r = 0

            future.set_result(r)

        mr_future.add_done_callback(sum_cb)
        return future

    @coroutine_annotation
    def aggregate_sum(self, field):
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

        fn_future = cursor.fetch_next
        future = get_future(self)

        def sum_cb(fn_future):
            if fn_future.result():
                doc = cursor.next_object()
                r = doc['total']
            else:
                r = 0

            future.set_result(r)

        fn_future.add_done_callback(sum_cb)
        return future

    @property
    @coroutine_annotation
    def fetch_next(self):
        return self._cursor.fetch_next

    def next_object(self):
        raw = self._cursor.next_object()
        return self._document._from_son(
            raw, _auto_dereference=self._auto_dereference,
            only_fields=self.only_fields)

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
            raise ConfusionError('Bad output type %r'.format(type(output)))

        return out

    def _check_delete_rules(self, doc, queryset, cascade_refs, write_concern):
        """Checks the delete rules for documents being deleted in a queryset.
        Raises an exception if any document has a DENY rule."""

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
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

        ret_future = get_future(self)

        # We need to set result for the future if there's no rules otherwise
        # the callbacks will never be called.
        if not delete_rules:
            ret_future.set_result(None)

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

                ref_q_count_future = ref_q.count()

                def count_cb(count_future):
                    try:
                        count = count_future.result()
                        if count > 0:
                            del_future = ref_q.delete(
                                write_concern=write_concern,
                                cascade_refs=cascade_refs)

                            def del_cb(del_future):
                                try:
                                    r = del_future.result()
                                    ret_future.set_result(r)
                                except Exception as e:
                                    ret_future.set_exception(e)

                            del_future.add_done_callback(del_cb)
                    except Exception as e:
                        ret_future.set_exception(e)

                ref_q_count_future.add_done_callback(count_cb)

            elif rule in (NULLIFY, PULL):
                if rule == NULLIFY:
                    updatekw = {'unset__%s' % field_name: 1}
                else:
                    updatekw = {'pull_all__%s' % field_name: self}

                update_future = document_cls.objects(
                    **{field_name + '__in': self}).update(
                        write_concern=write_concern, **updatekw)

                def update_cb(update_future):
                    try:
                        result = update_future.result()
                        ret_future.set_result(result)
                    except Exception as e:
                        ret_future.set_exception(e)

                update_future.add_done_callback(update_cb)

        return ret_future

    def _document_delete(self, queryset, write_concern):
        """Delete the documents in queryset by calling the document's delete
        method."""

        cnt = 0
        for doc in queryset:
            doc.delete(**write_concern)
            cnt += 1
        return cnt
