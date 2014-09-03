# -*- coding: utf-8 -*-

from tornado.testing import AsyncTestCase, gen_test
from mongomotor.monkey import patch_all
# patch things before
patch_all()

from mongoengine import signals
from mongoengine import Document
from mongoengine import StringField, ReferenceField
from tests.utils import dbconnect, dbdisconnect


class QuerySetTest(AsyncTestCase):
    def setUp(self):
        super(QuerySetTest, self).setUp()
        dbconnect()

        class Doc(Document):
            attr = StringField()

        class DocWithRef(Document):
            ref = ReferenceField(Doc)

        self.doc = Doc
        self.doc_with_ref = DocWithRef

    def tearDown(self):
        self.doc.drop_collection()
        dbdisconnect()

    @gen_test
    def test_in_bulk(self):
        doc1 = self.doc(attr='1')
        yield doc1.save()

        doc2 = self.doc(attr='2')
        yield doc2.save()

        objs = yield self.doc.objects.in_bulk([doc1.id, doc2.id])

        self.assertTrue(len(objs), 2)
        self.assertIn(doc1.id, objs)
        self.assertIn(doc2.id, objs)

        yield self.doc.drop_collection()

    @gen_test
    def test_all(self):

        d1 = self.doc(attr='asdf')
        yield d1.save()

        d2 = self.doc(attr='qwer')
        yield d2.save()

        objs = self.doc.objects.all()
        objs_len = yield objs.count()

        self.assertEqual(objs_len, 2)
        self.assertIn(d1, objs)
        self.assertIn(d2, objs)

        yield self.doc.drop_collection()

    @gen_test
    def test_filter(self):
        d1 = self.doc(attr='asdf')
        yield d1.save()

        d2 = self.doc(attr='qwer')
        yield d2.save()

        objs = self.doc.objects.filter(attr='asdf')
        objs_len = yield objs.count()

        objslist = yield objs.to_list()
        self.assertEqual(objs_len, 1)
        self.assertIn(d1, objslist)
        self.assertNotIn(d2, objslist)

        yield self.doc.drop_collection()


    @gen_test
    def test_all_getitem(self):
        d1 = self.doc(attr='asdf')
        yield d1.save()

        d2 = self.doc(attr='qwer')
        yield d2.save()

        obj = yield self.doc.objects.all()[0]

        self.assertEqual(obj, d1)

        yield self.doc.drop_collection()

    @gen_test
    def test_all_getitem_slice(self):
        d1 = self.doc(attr='asdf')
        yield d1.save()

        d2 = self.doc(attr='qwer')
        yield d2.save()

        d3 = self.doc(attr='zxcv')
        yield d3.save()

        objs = yield self.doc.objects.all()[0:2]
        count = yield objs.count()

        self.assertEqual(count, 2)
        self.assertEqual((yield objs[0]), d1)

        yield self.doc.drop_collection()

    @gen_test
    def test_get(self):
        d1 = self.doc(attr='asdf')
        yield d1.save()

        obj = yield self.doc.objects.get(attr='asdf')

        self.assertEqual(d1, obj)

        yield self.doc.drop_collection()

    @gen_test
    def test_delete(self):
        d1 = self.doc(attr='asdf')
        yield d1.save()

        yield d1.delete()

        with self.assertRaises(self.doc.objects._document.DoesNotExist):
            yield self.doc.objects.get(attr='asdf')

        yield self.doc.drop_collection()


    @gen_test
    def test_order_by(self):
        d1 = self.doc(attr='3')
        yield d1.save()

        d2 = self.doc(attr='2')
        yield d2.save()

        d3 = self.doc(attr='1')
        yield d3.save()

        objs = self.doc.objects.order_by('attr')

        self.assertEqual((yield objs[0]).id, d3.id)
        self.assertEqual((yield objs[1]).id, d2.id)
        self.assertEqual((yield objs[2]).id, d1.id)

        yield self.doc.drop_collection()

    @gen_test
    def test_insert(self):
        d = self.doc(attr='1')

        r = yield self.doc.objects.insert([d])

        d = yield self.doc.objects.get(id=r[0].id)

        self.assertTrue(d)

        yield self.doc.drop_collection()

    @gen_test
    def test_insert_with_post_bulk_insert_signal(self):

        def sigfunc(sender, documents, **kwargs):
            for document in documents:
                self.assertEqual((yield document.ref).__class__,
                                 self.doc_with_ref)

        signals.post_bulk_insert.connect(sigfunc, sender=self.doc_with_ref)

        ref = self.doc(attr='ref!')
        yield ref.save()

        dlist = [self.doc_with_ref(ref=ref) for x in range(2)]
        yield self.doc_with_ref.objects.insert(dlist)

    @gen_test
    def test_queryset_with_QCombination(self):
        pass
