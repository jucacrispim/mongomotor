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

from motor.metaprogramming import MotorAttributeFactory


class Sync(MotorAttributeFactory):

    def __init__(self, *args, **kwargs):
        """A descriptor that wraps a Motor method, such as insert or remove
        and returns the original PyMongo method. This is done because I want
        to asynchronize the whole mongoengine method, not only the PyMongo
        method.
        """

    def create_attribute(self, cls, attr_name):
        return getattr(cls.__delegate_class__, attr_name)
