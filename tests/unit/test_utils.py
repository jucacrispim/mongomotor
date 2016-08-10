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
from mongoengine import connection
from mongoengine.connection import get_db, connect, disconnect
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
        try:
            alias = 'bla'
            connect(alias=alias)
            db = get_db(alias)
            returned_alias = utils.get_alias_for_db(db)
            self.assertEqual(returned_alias, alias)
        finally:
            del connection._connections['bla']
