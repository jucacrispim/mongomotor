# -*- coding: utf-8 -*-

# thanks, gevent!

# all imports here are made inside functions or the
# queryset patch won't work

import imp


saved = {}


def patch_item(module, attr, newitem):
    NONE = object()
    olditem = getattr(module, attr, NONE)
    if olditem is not NONE:
        saved.setdefault(module.__name__, {}).setdefault(attr, olditem)
    setattr(module, attr, newitem)


def patch_connection():
    from mongoengine import connection
    from mongomotor.connection import get_connection

    patch_item(connection, 'get_connection', get_connection)


def patch_document():
    import mongoengine
    from mongomotor.base.document import BaseDocumentMotor as BaseDocument
    from mongomotor.document import Document, DynamicDocument, EmbeddedDocument

    patch_item(mongoengine, 'Document', Document)
    patch_item(mongoengine, 'DynamicDocument', DynamicDocument)
    patch_item(mongoengine, 'EmbeddedDocument', EmbeddedDocument)


def patch_queryset():
    import mongoengine
    from mongomotor.queryset.manager import QuerySetManager
    from mongomotor.queryset.queryset import QuerySet

    patch_item(mongoengine.base.metaclasses, 'QuerySetManager',
               QuerySetManager)
    patch_item(mongoengine.document, 'QuerySet', QuerySet)


def patch_transform():
    import mongoengine
    from mongomotor.queryset import transform

    patch_item(mongoengine.queryset.transform, 'query', transform.query)


def patch_visitor():
    import mongoengine
    from mongomotor.queryset import visitor

    patch_item(mongoengine.queryset.visitor, 'Q', visitor.Q)
    patch_item(mongoengine.queryset.visitor, 'QueryCompilerVisitor',
               visitor.QueryCompilerVisitor)

    patch_item(mongoengine.queryset.visitor, 'QueryCompilerVisitor',
               visitor.QueryCompilerVisitor)

    patch_item(mongoengine.queryset.visitor, 'QCombination',
               visitor.QCombination)

def patch_fields():
    import mongoengine
    from mongomotor.fields import ReferenceField

    patch_item(mongoengine, 'ReferenceField', ReferenceField)

def patch_dereference():
    import mongoengine
    from mongomotor.dereference import DeReferenceMotor

    patch_item(mongoengine.dereference, 'DeReference', DeReferenceMotor)

def patch_all():
    # the order here is important!
    patch_document()
    patch_fields()
    patch_transform()
    patch_visitor()
    patch_queryset()
    patch_dereference()

    patch_connection()


def _get_original(name, items):
    d = saved.get(name, {})
    values = []
    module = None
    for item in items:
        if item in d:
            values.append(d[item])
    return values


def get_original(name, item):
    if isinstance(item, str):
        return _get_original(name, [item])[0]
