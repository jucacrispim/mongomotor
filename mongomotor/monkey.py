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


class MonkeyPatcher:

    def __init__(self):
        self.patched = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for module, patches in self.patched.items():
            for attr, origobj in patches.items():
                setattr(module, attr, origobj)

    def patch_item(self, module, attr, newitem):
        NONE = object()
        olditem = getattr(module, attr, NONE)
        if olditem is not NONE:
            self.patched.setdefault(module, {}).setdefault(attr, olditem)
        setattr(module, attr, newitem)

    def patch_connection(self, client, replicaset_client):
        """Patches the db clients used to connect to mongodb.

        :param client: Which client should be used.
        :param replicaset_client: Which client should be used
          for replicasets."""
        from mongoengine import connection

        self.patch_item(connection, 'MongoClient', client)
        self.patch_item(connection, 'MongoReplicaSetClient', replicaset_client)
