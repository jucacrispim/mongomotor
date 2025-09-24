# -*- coding: utf-8 -*-

from bson.dbref import DBRef
from mongoengine.base import get_document
from mongoengine.connection import get_db
from mongoengine.dereference import DeReference
from .document import Document, EmbeddedDocument, TopLevelDocumentMetaclass
from .fields import ReferenceField, ListField, DictField, MapField
from .queryset import QuerySet


class MongoMotorDeReference(DeReference):

    async def __call__(self, items, max_depth=1, instance=None, name=None):
        """
        Cheaply dereferences the items to a set depth.
        Also handles the conversion of complex data types.

        :param items: The iterable (dict, list, queryset) to be dereferenced.
        :param max_depth: The maximum depth to recurse to
        :param instance: The owning instance used for tracking changes by
            :class:`~mongomotor.ComplexBaseField`
        :param name: The name of the field, used for tracking changes by
            :class:`~mongomotor.ComplexBaseField`
        :param get: A boolean determining if being called by __get__
        """
        if items is None or isinstance(items, str):
            return items

        if isinstance(items, QuerySet):
            items = await items.to_list()

        self.max_depth = max_depth
        doc_type = None

        if instance and isinstance(
            instance, (Document, EmbeddedDocument, TopLevelDocumentMetaclass)
        ):
            items, doc_type = self._get_deref_items_for_instance(
                instance, items, name)

        self.reference_map = self._find_references(items)
        self.object_map = await self._fetch_objects(doc_type=doc_type)
        return self._attach_objects(items, 0, instance, name)

    async def _fetch_objects(self, doc_type=None):
        """Fetch all references and convert to their document objects"""
        object_map = {}
        for collection, dbrefs in self.reference_map.items():
            # we use getattr instead of hasattr because hasattr swallows
            # any exception under python2
            # so it could hide nasty things without raising
            # exceptions (cfr bug #1688))
            ref_document_cls_exists = getattr(
                collection, "objects", None) is not None

            if ref_document_cls_exists:
                col_name = collection._get_collection_name()
                refs = [
                    dbref for dbref in dbrefs if (col_name, dbref)
                    not in object_map
                ]
                references = await collection.objects.in_bulk(refs)
                for key, doc in references.items():
                    object_map[(col_name, key)] = doc
            else:
                # Generic reference: use the refs data to convert to document
                if isinstance(doc_type, (ListField, DictField, MapField)):
                    continue

                refs = [
                    dbref for dbref in dbrefs if (collection, dbref)
                    not in object_map
                ]

                if doc_type:
                    references = doc_type._get_db()[collection].find(
                        {"_id": {"$in": refs}}
                    )
                    async for ref in references:
                        doc = doc_type._from_son(ref)
                        object_map[(collection, doc.id)] = doc
                else:
                    references = get_db()[collection].find(
                        {"_id": {"$in": refs}})
                    async for ref in references:
                        if "_cls" in ref:
                            doc = get_document(ref["_cls"])._from_son(ref)
                        elif doc_type is None:
                            doc = get_document(
                                "".join(x.capitalize()
                                        for x in collection.split("_"))
                            )._from_son(ref)
                        else:
                            doc = doc_type._from_son(ref)
                        object_map[(collection, doc.id)] = doc
        return object_map

    def _get_deref_items_for_instance(self, instance, items, name):

        doc_type = instance._fields.get(name)
        while hasattr(doc_type, "field"):
            doc_type = doc_type.field

        if not isinstance(doc_type, ReferenceField):
            return items, doc_type
        field = doc_type
        doc_type = doc_type.document_type
        is_list = not hasattr(items, "items")

        if is_list and all(i.__class__ == doc_type for i in items):
            return items, doc_type
        elif not is_list and all(
            i.__class__ == doc_type for i in items.values()
        ):
            return items, doc_type
        elif not field.dbref:
            # We must turn the ObjectIds into DBRefs

            # Recursively dig into the sub items of a list/dict
            # to turn the ObjectIds into DBRefs

            if not hasattr(items, "items"):
                items = _get_items_from_list(field, items)
            else:
                items = _get_items_from_dict(field, items)
        return items, doc_type


def _get_items_from_list(field, items):
    new_items = []
    for v in items:
        value = v
        if isinstance(v, dict):
            value = _get_items_from_dict(v)
        elif isinstance(v, list):
            value = _get_items_from_list(v)
        elif not isinstance(v, (DBRef, Document)):
            value = field.to_python(v)
        new_items.append(value)
    return new_items


def _get_items_from_dict(field, items):
    new_items = {}
    for k, v in items.items():
        value = v
        if isinstance(v, list):
            value = _get_items_from_list(v)
        elif isinstance(v, dict):
            value = _get_items_from_dict(v)
        elif not isinstance(v, (DBRef, Document)):
            value = field.to_python(v)
        new_items[k] = value
    return new_items
