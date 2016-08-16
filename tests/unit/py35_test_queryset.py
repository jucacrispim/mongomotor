# -*- coding: utf-8 -*-

# Copyright 2016 Juca Crispim <juca@poraodojuca.net>

# This file is part of mogomotor.

# mogomotor is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# mogomotor is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with mogomotor. If not, see <http://www.gnu.org/licenses/>.

# This file is only called in Python 3.5+

import sys
from unittest import TestCase
from tests import async_test
from mongomotor import Document, connect, disconnect
from mongomotor.fields import StringField


class PY35QuerySetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)
        connect(db)

    @classmethod
    def tearDownClass(cls):
        db = 'mongomotor-test-unit-{}{}'.format(sys.version_info.major,
                                                sys.version_info.minor)

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
        docs = [self.test_doc(str(i)) for i in range(4)]
        await self.test_doc.objects.insert(docs)

        async for doc in self.test_doc.objects:
            self.assertTrue(isinstance(doc, self.test_doc))
            self.assertTrue(doc.id)
