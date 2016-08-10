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

import sys
from unittest import TestCase
from unittest.mock import patch
import mongoengine
from mongomotor import Document, connect, disconnect
from mongomotor.fields import IntField
from tests import async_test


class DocumentTest(TestCase):

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
            i = IntField()

        self.test_doc = TestDoc

    @async_test
    def tearDown(self):
        yield from self.test_doc.drop_collection()

    @async_test
    def test_save(self):
        doc = self.test_doc(i=1)
        self.assertFalse(doc.id)
        yield from doc.save()
        self.assertTrue(doc.id)

    @patch('mongoengine.signals.post_delete')
    def test_delete(self, *args, **kwargs):
        doc = self.test_doc(i=1)
        yield from doc.save()

        yield from doc.delete()

        self.assertTrue(mongoengine.signals.post_delete.called)
