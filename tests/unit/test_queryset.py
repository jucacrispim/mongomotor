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
import sys
from unittest import TestCase
from mongomotor import Document, connect, disconnect
from mongomotor.fields import StringField, ListField, IntField
from mongomotor.queryset import (QuerySet, OperationError, Code,
                                 ConfusionError, SON, MapReduceDocument)
from tests import async_test


class QuerySetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        connect(db)

    @classmethod
    def tearDownClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)

        disconnect(db)

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
        futures = [self.test_doc(docint=i).save() for i in range(5)]
        asyncio.gather(*futures)
        average = yield from self.test_doc.objects.average('docint')
        self.assertEqual(average, 2)

    @async_test
    def test_aggregate_average(self):
        futures = [self.test_doc(docint=i).save() for i in range(5)]
        asyncio.gather(*futures)
        average = yield from self.test_doc.objects.aggregate_average('docint')
        self.assertEqual(average, 2)

    @async_test
    def test_sum(self):
        futures = [self.test_doc(docint=i).save() for i in range(5)]
        asyncio.gather(*futures)
        soma = yield from self.test_doc.objects.sum('docint')
        self.assertEqual(soma, 10)

    @async_test
    def test_aggregate_sum(self):
        futures = [self.test_doc(docint=i).save() for i in range(5)]
        asyncio.gather(*futures)
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
