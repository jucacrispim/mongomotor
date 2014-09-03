# -*- coding: utf-8 -*-

from bson.objectid import ObjectId
from mock import MagicMock
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from mongomotor import dereference
from mongomotor import Document
from mongomotor.fields import ReferenceField


class DeReferenceTest(AsyncTestCase):
    def setUp(self):
        super(DeReferenceTest, self).setUp()
        self.dereference = dereference.DeReference()
        class Ref(Document):
            pass

        class Doc(Document):
            f = ReferenceField(Ref)

        self.doc_class = Doc
        self.ref_class = Ref

    @gen_test
    def test_dereference_with_str(self):
        items = 'items'
        ret = yield self.dereference(items)

        self.assertEqual(ret, items)

    @gen_test
    def test_dereference_with_queryset(self):
        qs = self.doc_class.objects
        ret = yield self.dereference(qs)
        self.assertEqual(ret, [])

    @gen_test
    def test_dereference_with_referencefield(self):
        instance = self.doc_class()
        name = 'bla'
        ref = ReferenceField(self.doc_class)
        ref.to_python = MagicMock()
        ref.to_python.return_value = instance
        instance._fields = {name: ref}
        items = [ObjectId()]
        r = yield self.dereference(items, instance=instance, name=name)

        self.assertEqual(r, [instance])

    @gen_test
    def test_dereference_with_referencefield_and_is_list_and_all(self):
        # tests line 49/50 stmt.
        instance = self.doc_class()
        name = 'bla'
        ref = ReferenceField(self.doc_class)
        ref.to_python = MagicMock()
        ref.to_python.return_value = instance
        instance._fields = {name: ref}
        items = [ref.document_type()]
        r = yield self.dereference(items, instance=instance, name=name)

        self.assertEqual(r, [instance])

    @gen_test
    def test_dereference_with_referencefield_and_not_is_list_and_all(self):
        # tests line 51/52 stmt.
        instance = self.doc_class()
        name = 'bla'
        ref = ReferenceField(self.doc_class)
        ref.to_python = MagicMock()
        ref.to_python.return_value = instance
        instance._fields = {name: ref}
        items = {'doc': ref.document_type()}
        r = yield self.dereference(items, instance=instance, name=name)

        self.assertEqual(r, items)

    @gen_test
    def test_dereference_with_referencefield_on_dict(self):
        instance = self.doc_class()
        name = 'bla'
        ref = ReferenceField(self.doc_class)
        ref.to_python = MagicMock()
        ref.to_python.return_value = instance
        instance._fields = {name: ref}
        items = {'id': ObjectId()}
        r = yield self.dereference(items, instance=instance, name=name)

        self.assertTrue(isinstance(r['id'], self.doc_class))
