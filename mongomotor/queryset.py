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
from mongoengine.connection import get_db
from mongoengine.document import MapReduceDocument
from mongoengine.queryset.queryset import (QuerySet as BaseQuerySet,
                                           OperationError)
from mongomotor.exceptions import ConfusionError
from mongomotor.metaprogramming import (get_framework, AsyncGenericMetaclass,
                                        Async, asynchronize)


class QuerySet(BaseQuerySet, metaclass=AsyncGenericMetaclass):

    delete = Async()
    map_reduce = Async()

    def __repr__(self):
        return self.__class__.__name__

    def __getitem__(self, index):
        # It we received an slice we will return a queryset
        # so we will not touch the db now and do not need a future
        # here
        if isinstance(index, slice):
            return super().__getitem__(index)

        else:
            sync_getitem = BaseQuerySet.__getitem__
            async_getitem = asynchronize(sync_getitem)
            return async_getitem(self, index)

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

        framework = get_framework(self._document)
        loop = framework.get_event_loop()
        get_future = framework.get_future(loop)

        def _get_cb(future):
            docs = future.result()
            if len(docs) < 1:
                msg = ("%s matching query does not exist."
                       % queryset._document._class_name)
                get_future.set_exception(queryset._document.DoesNotExist(msg))

            elif len(docs) > 1:
                msg = 'More than 1 item returned'
                get_future.set_exception(
                    queryset._document.MultipleObjectsReturned(msg))
            else:
                get_future.set_result(docs[0])

        future = queryset.to_list(length=2)
        future.add_done_callback(_get_cb)
        return get_future

    def count(self, with_limit_and_skip=True):
        """Counts the documents in the queryset.

        :param with_limit_and_skip: Indicates if limit and skip applied to
          the queryset should be taken into account."""

        return super().count(with_limit_and_skip)

    def to_list(self, length=100):
        """Returns a list of the current documents in the queryset.

        :param length: maximum number of documents to return for this call."""

        cursor = self._cursor
        framework = get_framework(self._document)
        loop = framework.get_event_loop()
        list_future = framework.get_future(loop)

        def _to_list_cb(future):
            # Transforms mongo's raw documents into
            # mongomotor documents
            docs_list = future.result()
            final_list = [self._document._from_son(
                d, _auto_dereference=self._auto_dereference,
                only_fields=self.only_fields) for d in docs_list]

            list_future.set_result(final_list)

        future = cursor.to_list(length)
        future.add_done_callback(_to_list_cb)
        return list_future

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

        framework = get_framework(self)
        loop = framework.get_event_loop()
        future = framework.get_future(loop)

        def inline_mr_cb(result_future):
            result = result_future.result()
            gen = (MapReduceDocument(queryset._document, queryset._collection,
                                     doc['_id'], doc['value'])
                   for doc in result)
            future.set_result(gen)

        mr_future.add_done_callback(inline_mr_cb)
        return future

    @property
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
