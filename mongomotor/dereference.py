# -*- coding: utf-8 -*-

import tornado
from tornado import gen
from bson import DBRef
from mongoengine.queryset.queryset import QuerySet
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass
from mongoengine.fields import ReferenceField, ListField, DictField, MapField
from mongoengine import dereference, Document, EmbeddedDocument


class DeReference(dereference.DeReference):

    @gen.coroutine
    def __call__(self, items, max_depth=1, instance=None, name=None):
        """
        Cheaply dereferences the items to a set depth.
        Also handles the convertion of complex data types.

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

        if isinstance(items, QuerySet):
            items = yield items.to_list()

        self.max_depth = max_depth
        doc_type = None

        if instance and isinstance(instance, (Document, EmbeddedDocument,
                                              TopLevelDocumentMetaclass)):
            doc_type = instance._fields.get(name)
            if hasattr(doc_type, 'field'):
                doc_type = doc_type.field

            if isinstance(doc_type, ReferenceField):
                field = doc_type
                doc_type = doc_type.document_type
                is_list = not hasattr(items, 'items')
                if is_list and all([i.__class__ == doc_type for i in items]):
                    return items
                elif not is_list and all([i.__class__ == doc_type
                                         for i in list(items.values())]):
                    return items
                elif not field.dbref:
                    if not hasattr(items, 'items'):
                        items = [field.to_python(v)
                             if not isinstance(v, (DBRef, Document)) else v
                             for v in items]
                    else:
                        items = dict([
                            (k, field.to_python(v))
                            if not isinstance(v, (DBRef, Document)) else (k, v)
                            for k, v in items.items()]
                        )


        if isinstance(items, tornado.concurrent.Future):
            items = yield items

        self.reference_map = self._find_references(items)
        self.object_map = yield self._fetch_objects(doc_type=doc_type)
        return self._attach_objects(items, 0, instance, name)

    @gen.coroutine
    def _fetch_objects(self, doc_type=None):
        """Fetch all references and convert to their document objects
        """
        object_map = {}
        for col, dbrefs in self.reference_map.items():
            keys = list(object_map.keys())
            refs = list(set([dbref for dbref in dbrefs
                             if str(dbref).encode('utf-8') not in keys]))

            # We have a document class for the refs
            if hasattr(col, 'objects'):
                references = col.objects.in_bulk(refs)
                for key, doc in references.items():
                    object_map[key] = doc
            else:
                # Generic reference: use the refs data to convert to document
                if isinstance(doc_type, (ListField, DictField, MapField)):
                    continue

                if doc_type:
                    references = doc_type._get_db()[col].find(
                        {'_id': {'$in': refs}})
                    while (yield references.fetch_next):
                        ref = references.next_object()
                        doc = doc_type._from_son(ref)
                        object_map[doc.id] = doc
                else:
                    references = get_db()[col].find({'_id': {'$in': refs}})
                    for r in references:
                        ref = yield r
                        if '_cls' in ref:
                            doc = get_document(ref["_cls"])._from_son(ref)
                        elif doc_type is None:
                            doc = get_document(
                                ''.join(x.capitalize()
                                    for x in col.split('_')))._from_son(ref)
                        else:
                            doc = doc_type._from_son(ref)
                        object_map[doc.id] = doc
        return object_map
