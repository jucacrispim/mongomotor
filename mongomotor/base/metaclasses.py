# -*- coding: utf-8 -*-

import collections
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass, MetaDict
from mongoengine.errors import DoesNotExist, MultipleObjectsReturned
from mongoengine.fields import DynamicField
from mongomotor.queryset import QuerySetManager


class MapReduceDocumentMetaclass(TopLevelDocumentMetaclass):

    """ Metaclass for map-reduced collections.
    """

    def __new__(cls, name, bases, attrs):
        flattened_bases = cls._get_bases(bases)
        super_new = super(TopLevelDocumentMetaclass, cls).__new__

        # Set default _meta data if base class, otherwise get user defined meta

        if (attrs.get('my_metaclass') == MapReduceDocumentMetaclass):
            # defaults
            attrs['_meta'] = {
                'abstract': True,
                'max_documents': None,
                'max_size': None,
                'ordering': [],  # default ordering applied at runtime
                'indexes': [],  # indexes to be ensured at runtime
                'id_field': None,
                'index_background': False,
                'index_drop_dups': False,
                'index_opts': None,
                'delete_rules': None,
                'allow_inheritance': None,
            }
            attrs['_is_base_cls'] = True
            attrs['_meta'].update(attrs.get('meta', {}))
        else:
            attrs['_meta'] = attrs.get('meta', {})
            # Explictly set abstract to false unless set
            attrs['_meta']['abstract'] = attrs['_meta'].get('abstract', False)
            attrs['_is_base_cls'] = False

        # Set flag marking as document class - as opposed to an object mixin
        attrs['_is_document'] = True

        # Find the parent document class
        parent_doc_cls = [b for b in flattened_bases
                          if b.__class__ == MapReduceDocumentMetaclass]
        parent_doc_cls = None if not parent_doc_cls else parent_doc_cls[0]

        # Ensure abstract documents have abstract bases
        if attrs.get('_is_base_cls') or attrs['_meta'].get('abstract'):
            return super_new(cls, name, bases, attrs)

        # Merge base class metas.
        # Uses a special MetaDict that handles various merging rules
        meta = MetaDict()
        for base in flattened_bases[::-1]:
            # Add any mixin metadata from plain objects
            if hasattr(base, '_meta'):
                meta.merge(base._meta)

        meta.merge(attrs.get('_meta', {}))  # Top level meta

        # Set default collection name
        if 'collection' not in meta:
            meta['collection'] = ''.join('_%s' % c if c.isupper() else c
                                         for c in name).strip('_').lower()
        attrs['_meta'] = meta

        # Call super and get the new class
        new_class = super_new(cls, name, bases, attrs)

        meta = new_class._meta

        # Set index specifications
        meta['index_specs'] = new_class._build_index_specs(meta['indexes'])

        # Provide a default queryset unless exists or one has been set
        if 'objects' not in dir(new_class):
            new_class.objects = QuerySetManager()

        # Set primary key if not defined by the document
        new_class._auto_id_field = getattr(parent_doc_cls,
                                           '_auto_id_field', False)
        if not new_class._meta.get('id_field'):
            new_class._auto_id_field = True
            new_class._meta['id_field'] = 'id'
            new_class._fields['id'] = DynamicField(db_field='_id')
            new_class._fields['id'].name = 'id'
            new_class.id = new_class._fields['id']

        # Prepend id field to _fields_ordered
        if 'id' in new_class._fields and 'id' not in new_class._fields_ordered:
            new_class._fields_ordered = ('id', ) + new_class._fields_ordered

        # Merge in exceptions with parent hierarchy
        exceptions_to_merge = (DoesNotExist, MultipleObjectsReturned)
        module = attrs.get('__module__')
        for exc in exceptions_to_merge:
            name = exc.__name__
            parents = tuple(getattr(base, name) for base in flattened_bases
                            if hasattr(base, name)) or (exc,)
            # Create new exception and set to new_class
            exception = type(name, parents, {'__module__': module})
            setattr(new_class, name, exception)

        return new_class
