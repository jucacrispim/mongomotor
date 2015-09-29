# -*- coding: utf-8 -*-

from tornado import gen
from mongoengine.queryset import queryset
from mongomotor.queryset.base import BaseQuerySet


class QuerySet(BaseQuerySet, queryset.QuerySet):

    def __len__(self):
        """Since __len__ is called quite frequently (for example, as part of
        list(qs) we populate the result cache and cache the length.
        """

        raise NotImplementedError('Use count() - yielding it - instead')

    # sorry, does not work async
    def __repr__(self):
        return '<mongomotor.queryset.queryset.QuerySet instance>'

    def __iter__(self):
        """Iteration utilises a results cache which iterates the cursor
        in batches of ``ITER_CHUNK_SIZE``.

        If ``self._has_more`` the cursor hasn't been exhausted so cache then
        batch.  Otherwise iterate the result_cache.
        """

        if self._result_cache is None:
            self._result_cache = []

        while True:
            yield self._get_next()
            if not self._next_doc:
                raise StopIteration

    @gen.coroutine
    def __contains__(self, item):
        for o in self:
            obj = yield o
            if item == obj:
                return True
        return False

    @gen.coroutine
    def to_list(self):
        l = []
        for obj in self:
            try:
                doc = yield obj
            except StopIteration:
                continue
            if doc:
                doc = yield self._consume_references_futures(doc)
                l.append(doc)
        return l

    @gen.coroutine
    def _get_next(self):
        n = yield next(self)
        self._result_cache.append(n)
        return n

    def _iter_results(self):
        """A generator for iterating over the result cache.

        Also populates the cache if there are more possible results to yield.
        Raises StopIteration when there are no more results"""
        pos = 0
        while True:
            upper = len(self._result_cache)
            while pos < upper:
                yield self._result_cache[pos]
                pos = pos + 1
            if not self._has_more:
                raise StopIteration

    @gen.coroutine
    def _populate_cache(self):
        """
        Populates the result cache with ``ITER_CHUNK_SIZE`` more entries
        (until the cursor is exhausted).
        """
        if self._has_more:
            try:
                for i in range(queryset.ITER_CHUNK_SIZE):
                    n = yield next(self)
                    self._result_cache.append(n)
            except StopIteration:
                self._has_more = False
