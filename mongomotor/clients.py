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

from motor.frameworks import asyncio as asyncio_framework
from motor.frameworks import tornado as tornado_framework
from motor.metaprogramming import create_class_with_framework
from mongomotor.core import (MongoMotorAgnosticClient,
                             MongoMotorAgnosticReplicaSetClient)


MongoMotorAsyncIOClient = create_class_with_framework(MongoMotorAgnosticClient,
                                                      asyncio_framework,
                                                      'mongomotor.clients')

MongoMotorAsyncIOReplicaSetClient = create_class_with_framework(
    MongoMotorAgnosticReplicaSetClient,
    asyncio_framework,
    'mongomotor.clients')

MongoMotorTornadoClient = create_class_with_framework(MongoMotorAgnosticClient,
                                                      tornado_framework,
                                                      'mongomotor.clients')

MongoMotorTornadoReplicaSetClient = create_class_with_framework(
    MongoMotorAgnosticReplicaSetClient, tornado_framework,
    'mongomotor.clients')
