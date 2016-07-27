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
from motor.metaprogramming import create_class_with_framework
from motor.frameworks import asyncio as asyncio_framework
from mongomotor import metaprogramming


class SyncTest(TestCase):

    def test_create_attribute(self):
        class TestDelegate:

            def a(self):
                pass

        class TestClass:
            __motor_class_name__ = 'MyDelegateTest'
            __delegate_class__ = TestDelegate

            a = metaprogramming.Sync()

        tc = create_class_with_framework(TestClass, asyncio_framework,
                                         self.__module__)
        self.assertEqual(tc.a, TestDelegate.a)
