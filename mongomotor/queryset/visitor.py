# -*- coding: utf-8 -*-

import tornado
from tornado import gen
from mongoengine.queryset import visitor as visitor_module
from mongoengine.queryset import transform


class QueryCompilerVisitor(visitor_module.QueryCompilerVisitor):

    @gen.coroutine
    def visit_query(self, query):
        r = yield transform.query(self.document, **query.query)
        return r


class QNodeBase:
    @gen.coroutine
    def to_query(self, document):
        query = yield self.accept(visitor_module.SimplificationVisitor())
        query = yield query.accept(QueryCompilerVisitor(document))
        return query


class Q(QNodeBase, visitor_module.Q):

    @gen.coroutine
    def accept(self, visitor):
        r = yield visitor.visit_query(self)
        return r


class QCombination(QNodeBase, visitor_module.QCombination):

    @gen.coroutine
    def accept(self, visitor):
        for i in range(len(self.children)):
            if isinstance(self.children[i], visitor_module.QNode):
                accepted = self.children[i].accept(visitor)
                if isinstance(accepted, tornado.concurrent.Future):
                    accepted = yield accepted
                self.children[i] = accepted

        return visitor.visit_combination(self)
