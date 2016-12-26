# -*- coding: utf-8 -*-

from mongoengine.dereference import DeReference


class MongoMotorDeReference(DeReference):

    def _find_references(self, *args, **kwargs):
        """
        :param args: Positional args passed to DeReference._find_references.
        :param kwargs: Named args passed to DeReference._find_references.
        """

        r = super()._find_references(*args, **kwargs)
        return self._patch_in_bulk(r)

    def _patch_in_bulk(self, ref_map):
        """Changes the in_bulk method of the classes to
        the original pymongo method. It still uses the
        motor sockets and events.

        :param ref_map: Reference map. Keys are Document's subclasses
          and values are objects' ids.
        """

        new_map = {}
        for cls, value in ref_map.items():
            if not hasattr(cls, '_meta'):
                # it is not a Document, so skip it.
                new_map[cls] = value
                continue

            qs_class = cls._meta.get('queryset_class')
            qs_class = type("PatchedQS", (qs_class, ), {})
            sync_in_bulk = cls.objects.in_bulk.__wrapped__
            qs_class.in_bulk = sync_in_bulk
            new_cls_name = 'Patched{}'.format(cls.__name__)
            new_cls = type(new_cls_name, cls.__bases__, dict(cls.__dict__))
            new_cls._meta['queryset_class'] = qs_class
            new_map[new_cls] = value

        return new_map
