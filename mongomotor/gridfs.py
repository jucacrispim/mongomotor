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

from motor.metaprogramming import create_class_with_framework
from motor.motor_gridfs import AgnosticGridFS
from mongomotor.core import MongoMotorAgnosticDatabase
from mongomotor.metaprogramming import OriginalDelegate


class MongoMotorAgnosticGridFS(AgnosticGridFS):

    __motor_class_name__ = 'MongoMotorGridFS'

    delete = OriginalDelegate()
    get = OriginalDelegate()
    new_file = OriginalDelegate()
    put = OriginalDelegate()

    def __init__(self, database, collection="fs"):
        """An instance of GridFS on top of a single Database.

        :Parameters:
          - `database`: a :class:`~mongomotor.MongoMotorDatabase`
          - `collection` (optional): A string, name of root collection to use,
            such as "fs" or "my_files"

        .. mongodoc:: gridfs

        .. versionchanged:: 0.2
           ``open`` method removed; no longer needed.
        """

        db_class = create_class_with_framework(
            MongoMotorAgnosticDatabase, database._framework, self.__module__)

        if not isinstance(database, db_class):
            raise TypeError("First argument to MongoMotorGridFS must be "
                            "MongoMotorDatabase, not %r" % database)

        self.io_loop = database.get_io_loop()
        self.collection = database[collection]
        self.delegate = self.__delegate_class__(
            database.delegate,
            collection,
            _connect=False)

        self._GridFS__collection = self.delegate._GridFS__collection
        self._GridFS__ensure_index_files_id = \
            self.delegate._GridFS__ensure_index_files_id
        self._GridFS__files = self.delegate._GridFS__files
        self._GridFS__chunks = self.delegate._GridFS__chunks
