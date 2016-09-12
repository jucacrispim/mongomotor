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

import functools  # noqa: F401
import sys
import textwrap

PY35 = sys.version_info[:2] >= (3, 5)

if PY35:
    exec(textwrap.dedent("""
    if sys.version_info < (3, 5, 2):
        def aiter_compat(func):
            @functools.wraps(func)
            async def wrapper(self):
                return func(self)
            return wrapper
    else:
        def aiter_compat(func):
            return func
    """))
