# Copyright 2025 Juca Crispim <juca@poraodojuca.dev>

# This file is part of mongoengine.

# mongoengine is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# mongoengine is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with mongoengine. If not, see <http://www.gnu.org/licenses/>.

from mongoengine.context_managers import thread_locals

no_deref_cls_main = thread_locals.no_dereferencing_class


def no_dereferencing_active_for_class(cls):
    deref = getattr(thread_locals, 'no_dereferencing_class', None)
    if not deref:
        thread_locals.no_dereferencing_class = no_deref_cls_main
        deref = no_deref_cls_main

    return cls in deref
