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

from multiprocessing.pool import ThreadPool
from unittest import TestCase
from unittest.mock import Mock
from mongoengine import connection
from mongoengine.connection import disconnect
from mongomotor import utils


class GetSyncAliasTest(TestCase):

    def test_get_sync_alias(self):
        alias = 'some-conn'
        expected = 'some-conn-sync'
        returned = utils.get_sync_alias(alias)
        self.assertEqual(expected, returned)


class GetAliasForDbTest(TestCase):

    def tearDown(self):
        disconnect()
        super().tearDown()

    def test_get_alias(self):
        alias = 'bla'
        db = Mock()
        connection._dbs[alias] = db
        returned_alias = utils.get_alias_for_db(db)
        self.assertEqual(returned_alias, alias)


class ThreadingTest(TestCase):

    def test_is_main_thread_not_main(self):
        pool = ThreadPool(processes=1)
        r = pool.apply_async(utils.is_main_thread)

        pool.close()
        self.assertFalse(r.get())

    def test_is_main_thread_on_main(self):
        self.assertTrue(utils.is_main_thread())
