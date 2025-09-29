# -*- coding: utf-8 -*-
# Copyright 2025 Juca Crispim <juca@poraodojuca.dev>

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


from asyncblink import signal
pre_init = signal('pre_init')
post_init = signal('post_init')
pre_save = signal('pre_save')
post_save = signal('post_save')
pre_save_post_validation = signal('pre_save_post_validation')
pre_delete = signal('pre_delete')
post_delete = signal('post_delete')
pre_bulk_insert = signal('pre_bulk_insert')
post_bulk_insert = signal('post_bulk_insert')
