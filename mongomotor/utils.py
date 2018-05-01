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

import threading
from mongoengine.connection import _dbs


def get_sync_alias(alias):
    """Returns an alias to be used for the sync connection
    of alias."""

    return '{}-sync'.format(alias)


def get_alias_for_db(db):
    """Return the alias for a given db."""

    for alias, connected_db in _dbs.items():
        if db == connected_db:
            return alias


def is_main_thread():
    return threading.current_thread() == threading.main_thread()
