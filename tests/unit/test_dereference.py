# -*- coding: utf-8 -*-

from unittest import TestCase
from mongomotor import Document, disconnect
from mongomotor.fields import StringField, ReferenceField
from mongomotor.metaprogramming import asynchronize
from tests import async_test, connect2db


class MongoMotorDeReferenceTest(TestCase):

    @classmethod
    def setUpClass(cls):

        class TestRef(Document):
            attr = StringField()

        class TestCls(Document):
            someattr = StringField()
            ref = ReferenceField(TestRef)

        cls.test_ref = TestRef
        cls.test_cls = TestCls

        connect2db(async_framework='asyncio')

    @classmethod
    @async_test
    async def tearDownClass(cls):
        await cls.test_cls.drop_collection()
        await cls.test_ref.drop_collection()
        disconnect()

    @async_test
    async def test_patch_in_bulk(self):
        ref = self.test_ref(attr='bla')
        await ref.save()

        ref_map = {self.test_ref: ref.id}
        qs = self.test_cls.objects
        patched_refs = qs._dereference._patch_in_bulk(ref_map)
        for cls in patched_refs.keys():
            self.assertFalse(hasattr(cls.objects.in_bulk, '__wrapped__'))

    @async_test
    async def test_find_references(self):

        ref = self.test_ref(attr='bla')
        await ref.save()

        doc = self.test_cls(someattr='ble', ref=ref)
        await doc.save()

        qs = self.test_cls.objects

        def find_ref(*a, **kw):
            qs._dereference.max_depth = 1
            qs._dereference(*a, **kw)
            return qs._dereference.reference_map

        find_ref = asynchronize(find_ref)
        ref_map = await find_ref(qs)
        for doc in ref_map.keys():
            self.assertTrue(doc.__name__.startswith('Patched'))
