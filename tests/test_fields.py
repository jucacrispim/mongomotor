# -*- coding: utf-8 -*-

from mock import MagicMock, patch
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from mongomotor import Document
from mongomotor import fields
from mongomotor.fields import ReferenceField, StringField, DBRef
from mongomotor.base import fields as base_fields


class ReferenceFieldTest(AsyncTestCase):
    @patch.object(fields.ReferenceField, 'document_type', MagicMock())
    def setUp(self):
        super(ReferenceFieldTest, self).setUp()

        class RefDoc(Document):
            ble = StringField()

        class Doc(Document):
            bla = StringField()
            ref = ReferenceField(RefDoc)

        self.doc_class = Doc
        self.ref_class = RefDoc

    @gen_test
    def test_referencefield_on_class(self):
        ref = yield self.doc_class.ref

        self.assertEqual(ref.__class__, ReferenceField)

    @gen_test
    def test_referencefield_on_instance(self):
        doc = self.doc_class()
        ref = self.ref_class()
        doc.ref = ref

        ret = yield doc.ref

        self.assertEqual(ret, ref)

    def test_referencefield_with_wrong_arg_to_constructor(self):
        with self.assertRaises(Exception):
            r = ReferenceField([].__class__)

    @patch.object(fields.ReferenceField, 'document_type', MagicMock())
    @gen_test
    def test_referencefield_with_dbref(self):
        @gen.coroutine
        def fut(*args, **kw):
            return 1

        fields.ReferenceField.document_type._get_db.return_value\
                                                   .dereference = fut
        doc = self.doc_class()
        ref = self.ref_class()
        doc._data = MagicMock()
        doc._data.get.return_value = DBRef(collection='col', id='asdf')
        doc.ref = ref

        ret = yield doc.ref

        self.assertTrue(ret)


class ComplexBaseFieldTest(AsyncTestCase):
    def setUp(self):
        super(ComplexBaseFieldTest, self).setUp()
        class Doc(Document):
            f = base_fields.ComplexBaseField()

        self.doc_class = Doc

    @gen_test
    def test_complexbasefield_on_class(self):
        r = yield self.doc_class.f

        self.assertEqual(r.__class__, base_fields.ComplexBaseField)

    @patch.object(base_fields.fields.ComplexBaseField, '__get__', MagicMock())
    @gen_test
    def test_complexbasefield_with_list(self):
        @gen.coroutine
        def ret(*a, **kw):
            return [1]

        base_fields.fields.ComplexBaseField.__get__ = ret
        d = self.doc_class()
        d.f = [1]

        r = yield d.f
        self.assertEqual(r, [1])


    @patch.object(base_fields.fields.ComplexBaseField, '__get__', MagicMock())
    @gen_test
    def test_complexbasefield_with_dict(self):
        @gen.coroutine
        def ret(*a, **kw):
            return {'a': 1}

        base_fields.fields.ComplexBaseField.__get__ = ret
        d = self.doc_class()
        d.f = [1]

        r = yield d.f
        self.assertEqual(r, {'a': 1})
