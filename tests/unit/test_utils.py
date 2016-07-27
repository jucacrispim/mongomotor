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

import asyncio
from unittest import TestCase
from unittest.mock import patch
import tornado
from mongomotor import utils


class GetEventLoopTest(TestCase):

    @patch.object(utils, '_async_framework', 'asyncio')
    def test_get_event_loop_with_asyncio(self):
        loop = utils.get_event_loop()
        self.assertTrue(isinstance(loop, type(asyncio.get_event_loop())))

    @patch.object(utils, '_async_framework', 'tornado')
    def test_get_event_loop_with_tornado(self):
        loop = utils.get_event_loop()

        self.assertTrue(isinstance(
            loop, type(tornado.ioloop.IOLoop.instance())))

    def test_get_event_loop_with_bad_async(self):
        with self.assertRaises(utils.BadAsyncFrameworkError):
            utils.get_event_loop()
