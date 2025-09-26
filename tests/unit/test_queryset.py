# -*- coding: utf-8 -*-

# Copyright 2016, 2025 Juca Crispim <juca@poraodojuca.dev>

# This file is part of mongomotor.

# mongomotor is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# mongomotor is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with mongomotor. If not, see <http://www.gnu.org/licenses/>.

import asyncio
from unittest import TestCase
from unittest.mock import patch
import mongoengine
from mongomotor import Document, disconnect
from mongomotor.dereference import MongoMotorDeReference
from mongomotor.fields import StringField, ListField, IntField, ReferenceField
from mongomotor import queryset
from mongomotor.queryset import (QuerySet, Code)
from tests import async_test, connect2db


class QuerySetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db()

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        class TestDoc(Document):
            a = StringField()
            lf = ListField()
            docint = IntField()

        self.test_doc = TestDoc

    @async_test
    async def tearDown(self):
        await self.test_doc.drop_collection()

    @async_test
    async def test_to_list(self):
        futures = []
        for i in range(4):
            d = self.test_doc(a=str(i))
            futures.append(d.save())

        await asyncio.gather(*futures)
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)
        qs = qs.filter(a__in=['1', '2'])
        docs = await qs.to_list()
        self.assertEqual(len(docs), 2)
        self.assertTrue(isinstance(docs[0], self.test_doc))

    def test_dereference(self):
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)
        self.assertEqual(type(qs._dereference), MongoMotorDeReference)

    @async_test
    async def test_get(self):
        d = self.test_doc(a=str(1))
        await d.save()
        dd = self.test_doc(a=str(2))
        await dd.save()
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)

        returned = await qs.get(id=d.id)

        self.assertEqual(d.id, returned.id)

    @async_test
    async def test_get_with_no_doc(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(self.test_doc.DoesNotExist):
            await qs.get(a='bla')

    @async_test
    async def test_get_with_multiple_docs(self):
        d = self.test_doc(a='a')
        await d.save()
        d = self.test_doc(a='a')
        await d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        with self.assertRaises(self.test_doc.MultipleObjectsReturned):
            await qs.get(a='a')

    @async_test
    async def test_delete_queryset(self):
        d = self.test_doc(a='a')
        await d.save()
        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        await qs.delete()

        docs = await qs.to_list()
        self.assertEqual(len(docs), 0)

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    async def test_delete_with_rule_cascade(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.CASCADE)

            r = SomeRef()
            await r.save()
            d = SomeDoc(ref=r)
            await d.save()
            await r.delete()
            with self.assertRaises(SomeDoc.DoesNotExist):
                await SomeDoc.objects.get(id=d.id)
        finally:
            queryset._delete_futures = []
            await SomeRef.drop_collection()
            await SomeDoc.drop_collection()

    @async_test
    async def test_delete_with_multiple_rule_cascade(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.CASCADE)

            class OtherDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.CASCADE)

            r = SomeRef()
            await r.save()
            d = SomeDoc(ref=r)
            await d.save()
            await r.delete()
            with self.assertRaises(SomeDoc.DoesNotExist):
                await SomeDoc.objects.get(id=d.id)

        finally:
            queryset._delete_futures = []
            await SomeRef.drop_collection()
            await SomeDoc.drop_collection()

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    async def test_delete_with_rule_cascade_no_reference(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.CASCADE)

            r = SomeRef()
            await r.save()
            await r.delete()
            with self.assertRaises(SomeRef.DoesNotExist):
                await SomeRef.objects.get(id=r.id)
        finally:
            queryset._delete_futures = []
            await SomeRef.drop_collection()
            await SomeDoc.drop_collection()

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    async def test_delete_with_rule_nullify(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.NULLIFY)

            r = SomeRef()
            await r.save()
            d = SomeDoc(ref=r)
            await d.save()
            await r.delete()
            d = await SomeDoc.objects.get(id=d.id)
            self.assertIsNone((await d.ref))

        finally:
            queryset._delete_futures = []
            await SomeRef.drop_collection()
            await SomeDoc.drop_collection()

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    async def test_delete_with_rule_pull(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ListField(ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.PULL))

            r = SomeRef()
            await r.save()
            d = SomeDoc(ref=[r])
            await d.save()
            await r.delete()
            d = await SomeDoc.objects.get(id=d.id)
            self.assertEqual(len((await d.ref)), 0)
        finally:
            queryset._delete_futures = []
            await SomeRef.drop_collection()
            await SomeDoc.drop_collection()

    @async_test
    async def test_iterate_over_queryset(self):
        """Ensure that we can iterate over the queryset using
        fetch_next/next_doc.
        """

        for i in range(5):
            d = self.test_doc(a=str(i))
            await d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        c = 0
        async for doc in qs:
            c += 1

            self.assertIsInstance(doc, self.test_doc)

        self.assertEqual(c, 5)

    @async_test
    async def test_count_queryset(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            await d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)
        qs = qs.filter(a='1')
        count = await qs.count()
        self.assertEqual(count, 1)

    def test_queryset_len(self):
        with self.assertRaises(TypeError):
            len(self.test_doc.objects)

    @async_test
    async def test_getitem_with_slice(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            await d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        qs = qs[1:3]
        count = await qs.count()
        self.assertEqual(count, 2)

        incr = 0
        async for _ in qs:
            incr += 1

        self.assertEqual(incr, 2)

    @async_test
    async def test_getitem_with_int(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            await d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection).order_by('-a')

        doc = await qs[0]

        self.assertEqual(doc.a, '4')

    def test_code_with_str(self):
        code = 'function f(){return false}'
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        ret = qs._get_code(code)

        self.assertIsInstance(ret, Code)

    def test_code_with_code(self):
        code = Code('function f(){return false}')
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        ret = qs._get_code(code)

        self.assertIsInstance(ret, Code)
        self.assertIsNot(ret, code)

    @async_test
    async def test_item_frequencies(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        freq = await qs.item_frequencies('lf')
        self.assertEqual(freq['a'], 2)

    @async_test
    async def test_item_frequencies_with_normalize(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        freq = await qs.item_frequencies('lf', normalize=True)
        self.assertEqual(freq['a'], 0.5)

    @async_test
    async def test_average(self):
        docs = [self.test_doc(docint=i) for i in range(5)]
        await self.test_doc.objects.insert(docs)
        average = await self.test_doc.objects.average('docint')
        self.assertEqual(average, 2)

    @async_test
    async def test_map_reduce_inline(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        mapf = """
              function () {
                this.lf.forEach(function(z) {
                  emit(z, 1);
                });
              }
              """
        reducef = """
              function (key, values) {
                 var total = 0;
                 for (var i = 0; i < values.length; i++) {
                   total += values[i];
                 }
                 return total;
               }
               """
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        r = []
        async for doc in qs.map_reduce(mapf, reducef, 'inline'):
            assert isinstance(doc, mongoengine.document.MapReduceDocument)
            r.append(doc)

        assert len(r) == 3

    @async_test
    async def test_map_reduce_code_inline(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        mapf = queryset.Code("""
              function () {
                this.lf.forEach(function(z) {
                  emit(z, 1);
                });
              }
              """)
        reducef = queryset.Code("""
              function (key, values) {
                 var total = 0;
                 for (var i = 0; i < values.length; i++) {
                   total += values[i];
                 }
                 return total;
               }
               """)
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        r = []
        async for doc in qs.map_reduce(mapf, reducef, 'inline'):
            assert isinstance(doc, mongoengine.document.MapReduceDocument)
            r.append(doc)

        assert len(r) == 3

    @async_test
    async def test_map_reduce_collection_out(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        mapf = """
              function () {
                this.lf.forEach(function(z) {
                  emit(z, 1);
                });
              }
              """
        reducef = """
              function (key, values) {
                 var total = 0;
                 for (var i = 0; i < values.length; i++) {
                   total += values[i];
                 }
                 return total;
               }
               """
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        r = []
        async for doc in qs.map_reduce(mapf, reducef, 'mr_coll'):
            assert isinstance(doc, mongoengine.document.MapReduceDocument)
            r.append(doc)

        assert len(r) == 3

    @async_test
    async def test_map_reduce_replace_collection_out(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        mapf = """
              function () {
                this.lf.forEach(function(z) {
                  emit(z, 1);
                });
              }
              """
        reducef = """
              function (key, values) {
                 var total = 0;
                 for (var i = 0; i < values.length; i++) {
                   total += values[i];
                 }
                 return total;
               }
               """
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        r = []
        async for doc in qs.map_reduce(mapf, reducef, {"replace": 'mr_coll'}):
            assert isinstance(doc, mongoengine.document.MapReduceDocument)
            r.append(doc)

        assert len(r) == 3

    @async_test
    async def test_map_reduce_inline_finalize(self):
        d = self.test_doc(lf=['a', 'b'])
        await d.save()
        d = self.test_doc(lf=['a', 'c'])
        await d.save()

        mapf = """
              function () {
                this.lf.forEach(function(z) {
                  emit(z, 1);
                });
              }
              """
        reducef = """
              function (key, values) {
                 var total = 0;
                 for (var i = 0; i < values.length; i++) {
                   total += values[i];
                 }
                 return total;
               }
               """

        finalizef = """
               function (key, val) {return 0}
        """
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        r = []
        async for doc in qs.map_reduce(mapf, reducef, 'inline',
                                       finalize_f=finalizef):
            assert doc.value == 0
            r.append(doc)

        assert len(r) == 3

    @async_test
    async def test_aggregate(self):
        d = self.test_doc(a='a')
        await d.save()

        d = self.test_doc(a='b')
        await d.save()

        d = self.test_doc(a='c')
        await d.save()

        pipeline = [
            {"$sort": {"a": 1}},
            {"$project": {"_id": 0, "a": {"$toUpper": "$a"}}}
        ]
        expected = [{'a': 'A'}, {'a': 'B'}, {'a': 'C'}]
        returned = await (
            await self.test_doc.objects.aggregate(pipeline)
        ).to_list()

        self.assertEqual(returned, expected)

    @async_test
    async def test_sum(self):
        for i in range(5):
            d = self.test_doc(docint=i)
            await d.save()

        soma = await self.test_doc.objects.sum('docint')
        self.assertEqual(soma, 10)

    @async_test
    async def test_distinct(self):
        d = self.test_doc(a='a')
        await d.save()

        d = self.test_doc(a='a')
        await d.save()

        d = self.test_doc(a='b')
        await d.save()

        expected = ['a', 'b']
        returned = await self.test_doc.objects.distinct('a')

        self.assertEqual(returned, expected)

    @async_test
    async def test_first(self):
        d = self.test_doc(a='a')
        await d.save()

        d = self.test_doc(a='z')
        await d.save()

        f = await self.test_doc.objects.order_by('-a').first()
        self.assertEqual(f.a, 'z')

    @async_test
    async def test_explain(self):
        d = self.test_doc(a='a')
        await d.save()

        d = self.test_doc(a='z')
        await d.save()

        e = await self.test_doc.objects.order_by('-a').explain()
        assert e['explainVersion']

    @async_test
    async def test_first_with_empty_queryset(self):
        f = await self.test_doc.objects.first()
        self.assertIsNone(f)

    @async_test
    async def test_update_queryset(self):
        d = self.test_doc(a='a')
        await d.save()
        await self.test_doc.objects.filter(id=d.id).update(a='b')

        d = await self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.a, 'b')

    @async_test
    async def test_update_one(self):
        d = self.test_doc(a='a')
        await d.save()
        d = self.test_doc(a='a')
        await d.save()
        n_updated = await self.test_doc.objects.filter(
            a='a').update_one(a='b')

        self.assertEqual(n_updated, 1)
        self.assertEqual((await self.test_doc.objects(a='b').count()), 1)

    @async_test
    async def test_insert_documents(self):
        docs = [self.test_doc(a=str(i)) for i in range(3)]
        ret = await self.test_doc.objects.insert(docs)
        self.assertEqual(len(ret), 3)

    @async_test
    async def test_modify_document(self):
        d = self.test_doc(a='a')
        await d.save()

        await self.test_doc.objects.filter(id=d.id).modify(a='aa')
        d = await self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.a, 'aa')

    @async_test
    async def test_modify_upsert_document(self):
        await self.test_doc.objects.modify(upsert=True, a='zz')
        d = await self.test_doc.objects.get(a='zz')
        self.assertTrue(d.id)

    @async_test
    async def test_in_bulk(self):
        docs = []
        for i in range(5):
            d = self.test_doc(a=str(i))
            await d.save()
            docs.append(d)

        ret = await self.test_doc.objects.in_bulk([d.id for d in docs])
        self.assertEqual(len(ret), 5)

    @async_test
    async def test_upsert_one_update(self):
        docs = [self.test_doc(a='aa') for i in range(3)]
        docs = await self.test_doc.objects.insert(docs)
        first = docs[0]
        other = await self.test_doc.objects.upsert_one(a='zz')
        self.assertEqual(other.a, 'zz')
        self.assertEqual(first.id, other.id)

    @async_test
    async def test_upsert_one_insert(self):
        docs = [self.test_doc(a='aa') for i in range(3)]
        docs = await self.test_doc.objects.insert(docs)
        first = docs[0]
        other = await self.test_doc.objects(a='xx').upsert_one(a='zz')
        self.assertEqual(other.a, 'zz')
        self.assertNotEqual(first.id, other.id)

    @async_test
    async def test_create(self):
        doc = await self.test_doc.objects.create(a='123')
        self.assertTrue(doc.id)

    @async_test
    async def test_no_cache(self):
        await self.test_doc.objects.create(a='123')
        doc = await self.test_doc.objects.no_cache().get(a='123')
        self.assertTrue(doc.id)
        self.assertFalse(self.test_doc.objects._result_cache)

    # @async_test
    # async def test_explain(self):
    #     plan = await self.test_doc.objects.explain()
    #     self.assertFalse(isinstance(plan, asyncio.futures.Future))
