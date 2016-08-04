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

from unittest import TestCase
from mongomotor import Document, connect, disconnect
from mongomotor.fields import IntField
from tests import async_test


class DocumentTest(TestCase):

    @classmethod
    def setUpClass(cls):
        connect()

    @classmethod
    def tearDownClass(cls):
        disconnect()

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
