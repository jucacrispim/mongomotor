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
import tornado
from mongomotor.connection import _async_framework
from mongomotor.exceptions import BadAsyncFrameworkError


def get_event_loop():
    if _async_framework == 'asyncio':
        loop = asyncio.get_event_loop()

    elif _async_framework == 'tornado':
        loop = tornado.ioloop.IOLoop.instance()
    else:
        raise BadAsyncFrameworkError(_async_framework)

    return loop


def get_future(loop):
    if isinstance(loop, asyncio.events.AbstractEventLoop):
        future = asyncio.Future(loop=loop)
    elif isinstance(loop, tornado.ioloop.PollIOLoop):
        future = tornado.concurrent.Future()

    else:
        raise BadAsyncFrameworkError(_async_framework)

    return future
