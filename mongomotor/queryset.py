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

from mongoengine.queryset.queryset import QuerySet as BaseQuerySet
from mongomotor.metaprogramming import (get_framework, AsyncGenericMetaclass,
                                        Async)


class QuerySet(BaseQuerySet, metaclass=AsyncGenericMetaclass):

    delete = Async()

    def __repr__(self):
        return self.__class__.__name__

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

        future = self.to_list(length=2)
        future.add_done_callback(_get_cb)
        return get_future

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
