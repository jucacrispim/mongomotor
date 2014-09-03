# -*- coding: utf-8 -*-

from mock import Mock, patch
# this module raises an error in atexit. Importing it to mock
from tornado import concurrent
from tornado.testing import AsyncTestCase, gen_test
from mongomotor.monkey import patch_all
# patch things before
patch_all()

from mongoengine import signals
from mongoengine.document import NotUniqueError, OperationError
from mongoengine import Document, StringField, ReferenceField
from mongoengine.fields import URLField
from tests.utils import dbconnect, dbdisconnect


# mocking it to get rid of the atexit error
@patch.object(concurrent, 'futures', Mock())
class DocumentTest(AsyncTestCase):

    def setUp(self):
        super(DocumentTest, self).setUp()
        dbconnect()

        class UniqueDoc(Document):
            url = URLField(unique=True)

        class SimpleDoc(Document):
            attr = StringField(required=True)

        class Ref(Document):
            attr = StringField()

        class DocWithRef(Document):
            attr = StringField()
            ref = ReferenceField(Ref)

        self.unique_doc = UniqueDoc
        self.simple_doc = SimpleDoc
        self.doc_with_ref = DocWithRef
        self.ref = Ref

    def tearDown(self):
        self.unique_doc.drop_collection()
        self.simple_doc.drop_collection()
        self.doc_with_ref.drop_collection()
        self.ref.drop_collection()

        dbdisconnect()

    @gen_test
    def test_save(self):
        d = self.simple_doc(attr='bla')
        yield d.save()

        self.assertTrue(d.id)

    @gen_test
    def test_save_with_reference(self):

        r = self.ref(attr='ble')
        yield r.save()
        r.attr='bli'
        d = self.doc_with_ref(attr='bla', ref=r)

        yield d.save(cascade=True, _refs=[r])
        # need improve it, but i need to work on qs first
        self.assertEqual(r.attr, 'bli')

    @gen_test
    def test_save_force_insert(self):

        d = self.simple_doc(attr='bla')
        yield d.save(force_insert=True,
                     cascade_kwargs={'force_insert': True})

        self.assertTrue(d.id)

    @gen_test
    def test_save_update(self):
        class Doc(Document):
            attr = StringField()
            attr2 = StringField()

            meta = {'shard_key': ('attr',)}

        d = Doc(attr='bla', attr2='ble')
        yield d.save()

        d.attr2='bli'
        yield d.save()

        self.assertTrue(d.id)

        yield Doc.drop_collection()

    @gen_test
    def test_save_not_unique(self):

        d = self.unique_doc(url='http://nada.com')
        yield d.save()

        dd = self.unique_doc()
        dd.url = 'http://nada.com'
        with self.assertRaises(NotUniqueError):
            yield dd.save()

        yield self.unique_doc.drop_collection()

    @gen_test
    def test_delete(self):
        d = self.simple_doc(attr='asdf')
        yield d.save()

        yield d.delete()

        with self.assertRaises(self.simple_doc.DoesNotExist):
            yield self.simple_doc.objects.get(id=d.id)

        yield self.simple_doc.drop_collection()
