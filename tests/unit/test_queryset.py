# -*- coding: utf-8 -*-

# Copyright 2016 Juca Crispim <juca@poraodojuca.net>

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
import textwrap
from unittest import TestCase
from unittest.mock import patch
import mongoengine
from mongomotor import Document, disconnect
from mongomotor.dereference import MongoMotorDeReference
from mongomotor.fields import StringField, ListField, IntField, ReferenceField
from mongomotor import queryset
from mongomotor.queryset import (QuerySet, OperationError, Code,
                                 ConfusionError, SON, MapReduceDocument,
                                 PY35)
from tests import async_test, connect2db


class QuerySetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

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
    def tearDown(self):
        yield from self.test_doc.drop_collection()

    @async_test
    def test_to_list(self):
        futures = []
        for i in range(4):
            d = self.test_doc(a=str(i))
            futures.append(d.save())

        yield from asyncio.gather(*futures)
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)
        qs = qs.filter(a__in=['1', '2'])
        docs = yield from qs.to_list()
        self.assertEqual(len(docs), 2)
        self.assertTrue(isinstance(docs[0], self.test_doc))

    def test_dereference(self):
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)
        self.assertEqual(type(qs._dereference), MongoMotorDeReference)

    @async_test
    def test_get(self):
        d = self.test_doc(a=str(1))
        yield from d.save()
        dd = self.test_doc(a=str(2))
        yield from dd.save()
        collection = self.test_doc._collection
        qs = QuerySet(self.test_doc, collection)

        returned = yield from qs.get(id=d.id)

        self.assertEqual(d.id, returned.id)

    @async_test
    def test_get_with_no_doc(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(self.test_doc.DoesNotExist):
            yield from qs.get(a='bla')

    @async_test
    def test_get_with_multiple_docs(self):
        d = self.test_doc(a='a')
        yield from d.save()
        d = self.test_doc(a='a')
        yield from d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)
        with self.assertRaises(self.test_doc.MultipleObjectsReturned):
            yield from qs.get(a='a')

    @async_test
    def test_delete_queryset(self):
        d = self.test_doc(a='a')
        yield from d.save()
        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        yield from qs.delete()

        docs = yield from qs.to_list()
        self.assertEqual(len(docs), 0)

    def test_get_loop(self):
        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        loop = qs._get_loop()
        aio_loop = asyncio.get_event_loop()
        self.assertIs(loop, aio_loop)

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    def test_delete_with_rule_cascade(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.CASCADE)

            r = SomeRef()
            yield from r.save()
            d = SomeDoc(ref=r)
            yield from d.save()
            yield from r.delete()
            with self.assertRaises(SomeDoc.DoesNotExist):
                yield from SomeDoc.objects.get(id=d.id)
        finally:
            queryset._delete_futures = []
            yield from SomeRef.drop_collection()
            yield from SomeDoc.drop_collection()

    @async_test
    def test_delete_with_multiple_rule_cascade(self):
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
            yield from r.save()
            d = SomeDoc(ref=r)
            yield from d.save()
            yield from r.delete()
            with self.assertRaises(SomeDoc.DoesNotExist):
                yield from SomeDoc.objects.get(id=d.id)

        finally:
            queryset._delete_futures = []
            yield from SomeRef.drop_collection()
            yield from SomeDoc.drop_collection()

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    def test_delete_with_rule_cascade_no_reference(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.CASCADE)

            r = SomeRef()
            yield from r.save()
            yield from r.delete()
            with self.assertRaises(SomeRef.DoesNotExist):
                yield from SomeRef.objects.get(id=r.id)
        finally:
            queryset._delete_futures = []
            yield from SomeRef.drop_collection()
            yield from SomeDoc.drop_collection()

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    def test_delete_with_rule_nullify(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.NULLIFY)

            r = SomeRef()
            yield from r.save()
            d = SomeDoc(ref=r)
            yield from d.save()
            yield from r.delete()
            d = yield from SomeDoc.objects.get(id=d.id)
            self.assertIsNone((yield from d.ref))

        finally:
            queryset._delete_futures = []
            yield from SomeRef.drop_collection()
            yield from SomeDoc.drop_collection()

    @patch.object(queryset, 'TEST_ENV', True)
    @async_test
    def test_delete_with_rule_pull(self):
        try:
            class SomeRef(Document):
                pass

            class SomeDoc(Document):
                ref = ListField(ReferenceField(
                    SomeRef, reverse_delete_rule=mongoengine.PULL))

            r = SomeRef()
            yield from r.save()
            d = SomeDoc(ref=[r])
            yield from d.save()
            yield from r.delete()
            d = yield from SomeDoc.objects.get(id=d.id)
            self.assertEqual(len((yield from d.ref)), 0)
        finally:
            queryset._delete_futures = []
            yield from SomeRef.drop_collection()
            yield from SomeDoc.drop_collection()

    @async_test
    def test_iterate_over_queryset(self):
        """Ensure that we can iterate over the queryset using
        fetch_next/next_doc.
        """

        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        c = 0
        while (yield from qs.fetch_next):
            c += 1
            doc = qs.next_object()

            self.assertIsInstance(doc, self.test_doc)

        self.assertEqual(c, 5)

    @async_test
    def test_count_queryset(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)
        qs = qs.filter(a='1')
        count = yield from qs.count()
        self.assertEqual(count, 1)

    def test_queryset_len(self):
        with self.assertRaises(TypeError):
            len(self.test_doc.objects)

    @async_test
    def test_getitem_with_slice(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection)

        qs = qs[1:3]
        count = yield from qs.count()
        self.assertEqual(count, 2)

        incr = 0
        while (yield from qs.fetch_next):
            qs.next_object()
            incr += 1

        self.assertEqual(incr, 2)

    @async_test
    def test_getitem_with_int(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._get_collection()

        qs = QuerySet(self.test_doc, collection).order_by('-a')

        doc = yield from qs[0]

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

    def test_get_output_without_action_data(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(OperationError):
            qs._get_output({})

    def test_get_output_with_bad_output_type(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(ConfusionError):
            qs._get_output([])

    def test_get_output_with_son(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        son = SON({'merge': 'bla'})
        ret = qs._get_output(son)

        self.assertEqual(ret, son)

    def test_get_output_with_str(self):
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        ret = qs._get_output('outcoll')

        self.assertEqual(ret, 'outcoll')

    def test_get_output_with_dict(self):
        output = {'merge': 'bla', 'db_alias': 'default'}
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        ret = qs._get_output(output)
        self.assertIsInstance(ret, SON)

    @async_test
    def test_map_reduce_with_inline_output(self):
        # raises an exception when output is inline

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(OperationError):
            yield from qs.map_reduce('mapf', 'reducef', output='inline')

    @async_test
    def test_get_map_reduce(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        mapf = """
function(){
  emit(this.a, 1);
}
"""
        reducef = """
function(key, values){
  return Array.sum(values)
}
"""
        ret = yield from qs.map_reduce(mapf, reducef, {'merge': 'bla'})
        self.assertEqual(ret['counts']['input'], 5)

    @async_test
    def test_inline_map_reduce_with_bad_output(self):
        mr_kwrags = {'out': 'bla'}
        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        with self.assertRaises(OperationError):
            yield from qs.inline_map_reduce('mapf', 'reducef', **mr_kwrags)

    @async_test
    def test_inline_map_reduce(self):
        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        mapf = """
function(){
  emit(this.a, 1);
}
"""
        reducef = """
function(key, values){
  return Array.sum(values)
}
"""
        gen = yield from qs.inline_map_reduce(mapf, reducef)
        ret = list(gen)
        self.assertEqual(len(ret), 5)
        self.assertIsInstance(ret[0], MapReduceDocument)

    @async_test
    def test_item_frequencies(self):
        d = self.test_doc(lf=['a', 'b'])
        yield from d.save()
        d = self.test_doc(lf=['a', 'c'])
        yield from d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        freq = yield from qs.item_frequencies('lf')
        self.assertEqual(freq['a'], 2)

    @async_test
    def test_item_frequencies_with_normalize(self):
        d = self.test_doc(lf=['a', 'b'])
        yield from d.save()
        d = self.test_doc(lf=['a', 'c'])
        yield from d.save()

        collection = self.test_doc._get_collection()
        qs = QuerySet(self.test_doc, collection)

        freq = yield from qs.item_frequencies('lf', normalize=True)
        self.assertEqual(freq['a'], 0.5)

    @async_test
    def test_average(self):
        docs = [self.test_doc(docint=i) for i in range(5)]
        yield from self.test_doc.objects.insert(docs)
        average = yield from self.test_doc.objects.average('docint')
        self.assertEqual(average, 2)

    @async_test
    def test_aggregate_average(self):
        docs = [self.test_doc(docint=i) for i in range(5)]
        yield from self.test_doc.objects.insert(docs)
        average = yield from self.test_doc.objects.aggregate_average('docint')
        self.assertEqual(average, 2)

    @async_test
    def test_sum(self):
        for i in range(5):
            d = self.test_doc(docint=i)
            yield from d.save()

        soma = yield from self.test_doc.objects.sum('docint')
        self.assertEqual(soma, 10)

    @async_test
    def test_aggregate_sum(self):
        docs = [self.test_doc(docint=i) for i in range(5)]
        yield from self.test_doc.objects.insert(docs)
        soma = yield from self.test_doc.objects.aggregate_sum('docint')
        self.assertEqual(soma, 10)

    @async_test
    def test_distinct(self):
        d = self.test_doc(a='a')
        yield from d.save()

        d = self.test_doc(a='a')
        yield from d.save()

        d = self.test_doc(a='b')
        yield from d.save()

        expected = ['a', 'b']
        returned = yield from self.test_doc.objects.distinct('a')

        self.assertEqual(returned, expected)

    @async_test
    def test_first(self):
        d = self.test_doc(a='a')
        yield from d.save()

        d = self.test_doc(a='z')
        yield from d.save()

        f = yield from self.test_doc.objects.order_by('-a').first()
        self.assertEqual(f.a, 'z')

    @async_test
    def test_first_with_empty_queryset(self):
        f = yield from self.test_doc.objects.first()
        self.assertIsNone(f)

    @async_test
    def test_update_queryset(self):
        d = self.test_doc(a='a')
        yield from d.save()
        yield from self.test_doc.objects.filter(id=d.id).update(a='b')

        d = yield from self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.a, 'b')

    @async_test
    def test_update_one(self):
        d = self.test_doc(a='a')
        yield from d.save()
        d = self.test_doc(a='a')
        yield from d.save()
        n_updated = yield from self.test_doc.objects.filter(
            a='a').update_one(a='b')

        self.assertEqual(n_updated, 1)
        self.assertEqual((yield from self.test_doc.objects(a='b').count()), 1)

    @async_test
    def test_insert_documents(self):
        docs = [self.test_doc(a=str(i)) for i in range(3)]
        ret = yield from self.test_doc.objects.insert(docs)
        self.assertEqual(len(ret), 3)

    @async_test
    def test_modify_document(self):
        d = self.test_doc(a='a')
        yield from d.save()

        yield from self.test_doc.objects.filter(id=d.id).modify(a='aa')
        d = yield from self.test_doc.objects.get(id=d.id)
        self.assertEqual(d.a, 'aa')

    @async_test
    def test_modify_upsert_document(self):
        yield from self.test_doc.objects.modify(upsert=True, a='zz')
        d = yield from self.test_doc.objects.get(a='zz')
        self.assertTrue(d.id)

    @async_test
    def test_in_bulk(self):
        docs = []
        for i in range(5):
            d = self.test_doc(a=str(i))
            yield from d.save()
            docs.append(d)

        ret = yield from self.test_doc.objects.in_bulk([d.id for d in docs])
        self.assertEqual(len(ret), 5)

    @async_test
    def test_upsert_one_update(self):
        docs = [self.test_doc(a='aa') for i in range(3)]
        docs = yield from self.test_doc.objects.insert(docs)
        first = docs[0]
        other = yield from self.test_doc.objects.upsert_one(a='zz')
        self.assertEqual(other.a, 'zz')
        self.assertEqual(first.id, other.id)

    @async_test
    def test_upsert_one_insert(self):
        docs = [self.test_doc(a='aa') for i in range(3)]
        docs = yield from self.test_doc.objects.insert(docs)
        first = docs[0]
        other = yield from self.test_doc.objects(a='xx').upsert_one(a='zz')
        self.assertEqual(other.a, 'zz')
        self.assertNotEqual(first.id, other.id)

    @async_test
    def test_create(self):
        doc = yield from self.test_doc.objects.create(a='123')
        self.assertTrue(doc.id)

    @async_test
    def test_no_cache(self):
        yield from self.test_doc.objects.create(a='123')
        doc = yield from self.test_doc.objects.no_cache().get(a='123')
        self.assertTrue(doc.id)
        self.assertFalse(self.test_doc.objects._result_cache)

    # @async_test
    # def test_explain(self):
    #     plan = yield from self.test_doc.objects.explain()
    #     self.assertFalse(isinstance(plan, asyncio.futures.Future))


class PY35QuerySetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect2db(async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        class TestDoc(Document):
            a = StringField()

        self.test_doc = TestDoc

    @async_test
    def tearDown(self):
        yield from self.test_doc.drop_collection()

    @async_test
    async def test_async_iterate_queryset(self):
        docs = [self.test_doc(a=str(i)) for i in range(4)]
        await self.test_doc.objects.insert(docs)

        async for doc in self.test_doc.objects:
            self.assertTrue(isinstance(doc, self.test_doc))
            self.assertTrue(doc.id)

        count = await self.test_doc.objects.count()
        self.assertEqual(count, 4)
