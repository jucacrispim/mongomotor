# -*- coding: utf-8 -*-

from bson import DBRef
from mongoengine.dereference import DeReference
from mongoengine.base import get_document, TopLevelDocumentMetaclass
from mongoengine.fields import (ListField, DictField, MapField,
                                ReferenceField)
from mongoengine.connection import get_db
from mongomotor.queryset import QuerySet
from mongomotor.document import Document, EmbeddedDocument
import tornado


class DeReferenceMotor(DeReference):

    @tornado.gen.coroutine
    def __call__(self, items, max_depth=1, instance=None, name=None):
        """
        Cheaply dereferences the items to a set depth.
        Also handles the conversion of complex data types.

        :param items: The iterable (dict, list, queryset) to be dereferenced.
        :param max_depth: The maximum depth to recurse to
        :param instance: The owning instance used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param name: The name of the field, used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param get: A boolean determining if being called by __get__
        """
        if items is None or isinstance(items, str):
            return items

        # cheapest way to convert a queryset to a list
        # list(queryset) uses a count() query to determine length
        if isinstance(items, QuerySet):
            items = [i for i in items]

        self.max_depth = max_depth
        doc_type = None

        if instance and isinstance(instance, (Document, EmbeddedDocument,
                                              TopLevelDocumentMetaclass)):
            doc_type = instance._fields.get(name)
            while hasattr(doc_type, 'field'):
                doc_type = doc_type.field

            if isinstance(doc_type, ReferenceField):
                field = doc_type
                doc_type = doc_type.document_type
                is_list = not hasattr(items, 'items')

                if is_list and all([i.__class__ == doc_type for i in items]):
                    return items
                elif not is_list and all(
                        [i.__class__ == doc_type for i in list(items.values())]):
                    return items
                elif not field.dbref:
                    if not hasattr(items, 'items'):

                        def _get_items(items):
                            new_items = []
                            for v in items:
                                if isinstance(v, list):
                                    new_items.append(_get_items(v))
                                elif not isinstance(v, (DBRef, Document)):
                                    new_items.append(field.to_python(v))
                                else:
                                    new_items.append(v)
                            return new_items

                        items = _get_items(items)
                    else:
                        items = dict([
                            (k, field.to_python(v))
                            if not isinstance(v, (DBRef, Document)) else (k, v)
                            for k, v in items.items()]
                        )

        self.reference_map = self._find_references(items)
        self.object_map = yield self._fetch_objects(doc_type=doc_type)
        return self._attach_objects(items, 0, instance, name)

    @tornado.gen.coroutine
    def _fetch_objects(self, doc_type=None):
        """Fetch all references and convert to their document objects
        """
        object_map = {}
        for collection, dbrefs in self.reference_map.items():
            keys = list(object_map.keys())
            refs = list(
                set([dbref for dbref in dbrefs if str(dbref).encode('utf-8')
                     not in keys]))
            if hasattr(collection, 'objects'):
                # We have a document class for the refs
                references = yield collection.objects.in_bulk(refs)
                for key, doc in references.items():
                    object_map[key] = doc
            else:
                # Generic reference: use the refs data to convert to document
                if isinstance(doc_type, (ListField, DictField, MapField)):
                    continue

                if doc_type:
                    references = doc_type._get_db()[collection].find(
                        {'_id': {'$in': refs}})

                    next_ref = yield self._get_next_doc(references)
                    while next_ref:
                        doc = doc_type._from_son(next_ref)
                        object_map[(collection, doc.id)] = doc
                        next_ref = yield self._get_next_doc(references)

                else:
                    references = get_db()[collection].find(
                        {'_id': {'$in': refs}})
                    ref = yield self._get_next_doc(references)
                    while ref:
                        if '_cls' in ref:
                            doc = get_document(ref["_cls"])._from_son(ref)
                        elif doc_type is None:
                            doc = get_document(
                                ''.join(x.capitalize()
                                        for x in collection.split('_')))
                            doc = doc._from_son(ref)
                        else:
                            doc = doc_type._from_son(ref)
                        object_map[(collection, doc.id)] = doc
                        ref = yield self._get_next_doc(references)

        return object_map

    @tornado.gen.coroutine
    def _get_next_doc(self, cursor):
        yield cursor.fetch_next
        n = cursor.next_object()
        return n
