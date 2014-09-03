# -*- coding: utf-8 -*-

from mongoengine.queryset import manager
from mongomotor.queryset.queryset import QuerySet


class QuerySetManager(manager.QuerySetManager):
    default = QuerySet
