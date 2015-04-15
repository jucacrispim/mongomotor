# -*- coding: utf-8 -*-

from bson import DBRef, SON
from mongoengine.dereference import DeReference
from mongoengine.base import (
    BaseDict, BaseList, EmbeddedDocumentList,
    get_document
)
from mongoengine.fields import (ListField, DictField, MapField)
from mongoengine.connection import get_db

from mongomotor.fields import ListField, DictField, MapField
from mongomotor.document import Document, EmbeddedDocument
from motor import MotorCursor
import tornado


class DeReferenceMotor(DeReference):

    def _fetch_objects(self, doc_type=None):
        """Fetch all references and convert to their document objects
        """
        object_map = {}
        for collection, dbrefs in self.reference_map.items():
            keys = list(object_map.keys())
            refs = list(set([dbref for dbref in dbrefs if str(dbref).encode('utf-8') not in keys]))
            if hasattr(collection, 'objects'):  # We have a document class for the refs
                references = collection.objects.in_bulk(refs)
                for key, doc in references.items():
                    object_map[key] = doc
            else:  # Generic reference: use the refs data to convert to document
                if isinstance(doc_type, (ListField, DictField, MapField)):
                    continue

                if doc_type:
                    # MONGOMOTOR HERE! Making lists of references became lists of futures
                    references = doc_type._get_db()[collection].find({'_id': {'$in': refs}})
                    if isinstance(references, MotorCursor):
                        object_map = self._async_dereference(references, doc_type)
                    else:
                        for ref in references:
                            doc = doc_type._from_son(ref)
                            object_map[doc.id] = doc
                else:
                    references = get_db()[collection].find({'_id': {'$in': refs}})
                    for ref in references:
                        if '_cls' in ref:
                            doc = get_document(ref["_cls"])._from_son(ref)
                        elif doc_type is None:
                            doc = get_document(
                                ''.join(x.capitalize()
                                    for x in collection.split('_')))._from_son(ref)
                        else:
                            doc = doc_type._from_son(ref)
                        object_map[doc.id] = doc
        return object_map

    def _attach_objects(self, items, depth=0, instance=None, name=None):
        """
        Recursively finds all db references to be dereferenced

        :param items: The iterable (dict, list, queryset)
        :param depth: The current depth of recursion
        :param instance: The owning instance used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        :param name: The name of the field, used for tracking changes by
            :class:`~mongoengine.base.ComplexBaseField`
        """

        if isinstance(self.object_map, tornado.concurrent.Future):
            return self._async_attach_objects(items, depth, instance, name)
        else:
            return self._actually_attach_objects(items, depth, instance, name)

    @tornado.gen.coroutine
    def _async_attach_objects(self, items, depth, instance, name):
        self.object_map = yield self.object_map
        return self._actually_attach_objects(items, depth, instance, name)

    def _actually_attach_objects(self, items, depth=0, instance=None,
                                 name=None):
        if not items:
            if isinstance(items, (BaseDict, BaseList)):
                return items

            if instance:
                if isinstance(items, dict):
                    return BaseDict(items, instance, name)
                else:
                    return BaseList(items, instance, name)

        if isinstance(items, (dict, SON)):
            if '_ref' in items:
                return self.object_map.get(items['_ref'].id, items)
            elif '_cls' in items:
                doc = get_document(items['_cls'])._from_son(items)
                _cls = doc._data.pop('_cls', None)
                del items['_cls']
                doc._data = self._attach_objects(doc._data, depth, doc, None)
                if _cls is not None:
                    doc._data['_cls'] = _cls
                return doc

        if not hasattr(items, 'items'):
            is_list = True
            list_type = BaseList
            if isinstance(items, EmbeddedDocumentList):
                list_type = EmbeddedDocumentList
            as_tuple = isinstance(items, tuple)
            iterator = enumerate(items)
            data = []
        else:
            is_list = False
            iterator = iter(items.items())
            data = {}

        depth += 1
        for k, v in iterator:
            if is_list:
                data.append(v)
            else:
                data[k] = v

            if k in self.object_map and not is_list:
                data[k] = self.object_map[k]
            elif isinstance(v, (Document, EmbeddedDocument)):
                for field_name, field in v._fields.items():
                    v = data[k]._data.get(field_name, None)
                    if isinstance(v, (DBRef)):
                        data[k]._data[field_name] = self.object_map.get(v.id, v)
                    elif isinstance(v, (dict, SON)) and '_ref' in v:
                        data[k]._data[field_name] = self.object_map.get(v['_ref'].id, v)
                    elif isinstance(v, dict) and depth <= self.max_depth:
                        data[k]._data[field_name] = self._attach_objects(v, depth, instance=instance, name=name)
                    elif isinstance(v, (list, tuple)) and depth <= self.max_depth:
                        data[k]._data[field_name] = self._attach_objects(v, depth, instance=instance, name=name)
            elif isinstance(v, (dict, list, tuple)) and depth <= self.max_depth:
                item_name = '%s.%s' % (name, k) if name else name
                data[k] = self._attach_objects(v, depth - 1, instance=instance, name=item_name)
            elif hasattr(v, 'id'):
                data[k] = self.object_map.get(v.id, v)

        if instance and name:
            if is_list:
                return tuple(data) if as_tuple else list_type(data, instance, name)
            return BaseDict(data, instance, name)
        depth += 1
        return data

    @tornado.gen.coroutine
    def _async_dereference(self, references, doc_type):
        object_map = {}
        next_ref = yield self._get_next_doc(references)
        while next_ref:
            doc = doc_type._from_son(next_ref)
            object_map[doc.id] = doc
            next_ref = yield self._get_next_doc(references)

        return object_map

    @tornado.gen.coroutine
    def  _get_next_doc(self, cursor):
        yield cursor.fetch_next
        n = cursor.next_object()
        return  n
