# -*- coding: utf-8 -*-

# Copyright 2016-2017 Juca Crispim <juca@poraodojuca.net>

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

from motor.frameworks import asyncio as asyncio_framework
try:
    from motor.frameworks import tornado as tornado_framework
except ImportError:
    tornado_framework = None
from motor.metaprogramming import create_class_with_framework
from mongomotor.core import MongoMotorAgnosticClient
from mongomotor.exceptions import MissingFramework


MongoMotorAsyncIOClient = create_class_with_framework(MongoMotorAgnosticClient,
                                                      asyncio_framework,
                                                      'mongomotor.clients')


class DummyMongoMotorTornadoClient:
    """A dummy class to raise an exception when creating an instance
    warning about the absense of tornado. """

    def __init__(self, *args, **kwargs):
        msg = 'tornado framework is not present. '
        msg += 'Did you installed tornado?'
        raise MissingFramework(msg)


if tornado_framework:
    MongoMotorTornadoClient = create_class_with_framework(
        MongoMotorAgnosticClient, tornado_framework, 'mongomotor.clients')

else:
    MongoMotorTornadoClient = DummyMongoMotorTornadoClient
