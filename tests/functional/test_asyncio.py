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


import unittest
from bson.objectid import ObjectId
import sys
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test
from mongoengine.errors import OperationError
from mongomotor import connect, disconnect
from mongomotor import Document, EmbeddedDocument, MapReduceDocument
from mongomotor.fields import (StringField, IntField, ListField, DictField,
                               EmbeddedDocumentField, ReferenceField)

from tests import async_test

db = 'mongomotor-test-{}'.format(sys.version_info.major,
                                 sys.version_info.minor)


class MongoMotorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        connect(db, async_framework='asyncio')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def setUp(self):
        super(MongoMotorTest, self).setUp()

        # some models to simple tests over
        # mongomotor

        class EmbedRef(EmbeddedDocument):
            list_field = ListField()

        class RefDoc(Document):
            refname = StringField()
            embedlist = ListField(EmbeddedDocumentField(EmbedRef))

        class SuperDoc(Document):
            some_field = StringField()
            reflist = ListField(ReferenceField(RefDoc))

            meta = {'allow_inheritance': True}

        class OtherDoc(SuperDoc):
            pass

        class Embed(EmbeddedDocument):
            dict_field = DictField()

        class MainDoc(SuperDoc):
            docname = StringField()
            docint = IntField()
            list_field = ListField(StringField())
            embedded = EmbeddedDocumentField(Embed)
            ref = ReferenceField(RefDoc)

        self.maindoc = MainDoc
        self.embed = Embed
        self.refdoc = RefDoc
        self.embedref = EmbedRef
        self.otherdoc = OtherDoc

    @async_test
    def tearDown(self):
        yield from self.maindoc.drop_collection()
        yield from self.refdoc.drop_collection()
        yield from self.otherdoc.drop_collection()
        yield from self.refdoc.drop_collection()

        super(MongoMotorTest, self).tearDown()

    @async_test
    def test_create(self):
        """Ensure that a new document can be added into the database
        """
        embedref = self.embedref(list_field=['uma', 'lista', 'nota', 10])
        ref = self.refdoc(refname='refname', embedlist=[embedref])

        yield from ref.save()

        # asserting if our reference document was created
        self.assertTrue(ref.id)
        # and if the listfield is ok
        embedlist = ref.embedlist
        self.assertEqual(embedlist[0].list_field, ['uma', 'lista', 'nota', 10])

        # creating the main document
        embed = self.embed(dict_field={'key': 'value'})
        main = self.maindoc(docname='docname', docint=1)
        main.list_field = ['list', 'of', 'strings']
        main.embedded = embed
        main.ref = ref
        yield from main.save()

        # asserting if our main document was created
        self.assertTrue(main.id)
        # and if the reference points to the right place.
        # note that you need to yield reference fields.
        self.assertEqual((yield from main.ref), ref)

    @async_test
    def test_save_with_no_ref(self):
        """Ensure that a document which has a ReferenceField can
        be saved with the referece being None.
        """
        # remebering from a wired bug
        # the thing is: on document constructor mongoengine tries to
        # set default values and that makes an None reference became
        # a future.
        doc = self.maindoc()
        yield from doc.save()
        self.assertIsNone((yield from doc.ref))
