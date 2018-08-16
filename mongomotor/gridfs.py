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

import gridfs
from gridfs import grid_file
from motor.metaprogramming import create_class_with_framework
from motor.motor_gridfs import (AgnosticGridFSBucket, AgnosticGridIn,
                                AgnosticGridOut, AgnosticGridOutCursor)
from mongomotor.core import (MongoMotorAgnosticDatabase,
                             MongoMotorAgnosticCollection)
from mongomotor.metaprogramming import OriginalDelegate


class MongoMotorAgnosticGridOut(AgnosticGridOut):
    """Class to read data out of GridFS.

    MotorGridOut supports the same attributes as PyMongo's
    :class:`~gridfs.grid_file.GridOut`, such as ``_id``, ``content_type``,
    etc.

    You don't need to instantiate this class directly - use the
    methods provided by :class:`~mongomotor.MotorGridFSBucket`. If it **is**
    instantiated directly, call :meth:`open`, :meth:`read`, or
    :meth:`readline` before accessing its attributes.
    """
    __motor_class_name__ = 'MongoMotorGridOut'

    def __init__(self, root_collection, file_id=None, file_document=None,
                 delegate=None):
        collection_class = create_class_with_framework(
            MongoMotorAgnosticCollection, self._framework, self.__module__)

        if not isinstance(root_collection, collection_class):
            raise TypeError(
                "First argument to MongoMotorGridOut must be "
                "MongoMotorCollection, not %r" % root_collection)

        if delegate:
            self.delegate = delegate
        else:
            self.delegate = self.__delegate_class__(
                root_collection.delegate,
                file_id,
                file_document)

        self.io_loop = root_collection.get_io_loop()


class MongoMotorAgnosticGridIn(AgnosticGridIn):

    __motor_class_name__ = 'MongoMotorGridIn'

    def __init__(self, root_collection, delegate=None, **kwargs):
        """
        Class to write data to GridFS. Application developers should not
        generally need to instantiate this class - see
        :meth:`~mongomotor.MongoMotorGridFSBucket.open_upload_stream`.

        Any of the file level options specified in the `GridFS Spec
        <http://dochub.mongodb.org/core/gridfs>`_ may be passed as
        keyword arguments. Any additional keyword arguments will be
        set as additional fields on the file document. Valid keyword
        arguments include:

          - ``"_id"``: unique ID for this file (default:
            :class:`~bson.objectid.ObjectId`) - this ``"_id"`` must
            not have already been used for another file

          - ``"filename"``: human name for the file

          - ``"contentType"`` or ``"content_type"``: valid mime-type
            for the file

          - ``"chunkSize"`` or ``"chunk_size"``: size of each of the
            chunks, in bytes (default: 256 kb)

          - ``"encoding"``: encoding used for this file. In Python 2,
            any :class:`unicode` that is written to the file will be
            converted to a :class:`str`. In Python 3, any :class:`str`
            that is written to the file will be converted to
            :class:`bytes`.

        :Parameters:
          - `root_collection`: A :class:`~mongomotor.MongoMotorCollection`,
             the root collection to write to.
          - `delegate`: An instance of the delegate class.
          - `**kwargs` (optional): file level options (see above)

        """
        collection_class = create_class_with_framework(
            MongoMotorAgnosticCollection, self._framework, self.__module__)

        if not isinstance(root_collection, collection_class):
            raise TypeError(
                "First argument to MotorGridIn must be "
                "MongoMotorCollection, not %r" % root_collection)

        self.io_loop = root_collection.get_io_loop()
        if delegate:
            # Short cut.
            self.delegate = delegate
        else:
            self.delegate = self.__delegate_class__(
                root_collection.delegate,
                **kwargs)


class MongoMotorAgnosticGridFS(AgnosticGridFSBucket):
    """Create a handle to a GridFS bucket.

    Raises :exc:`~pymongo.errors.ConfigurationError` if `write_concern`
    is not acknowledged.

    This class conforms to the `GridFS API Spec
    <https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst>`_
    for MongoDB drivers.

    :Parameters:
      - `database`: database to use.
      - `bucket_name` (optional): The name of the bucket. Defaults to 'fs'.
      - `chunk_size_bytes` (optional): The chunk size in bytes. Defaults
        to 255KB.
      - `write_concern` (optional): The
        :class:`~pymongo.write_concern.WriteConcern` to use. If ``None``
        (the default) db.write_concern is used.
      - `read_preference` (optional): The read preference to use. If
        ``None`` (the default) db.read_preference is used.
      - `disable_md5` (optional): When True, MD5 checksums will not be
        computed for uploaded files. Useful in environments where MD5
        cannot be used for regulatory or other reasons. Defaults to False.

    .. mongodoc:: gridfs
    """

    __motor_class_name__ = 'MongoMotorGridFS'

    def __init__(self, database, collection="fs"):
        """An instance of GridFS on top of a single Database.

        :Parameters:
          - `database`: a :class:`~mongomotor.MongoMotorDatabase`
          - `collection` (optional): A string, name of root collection to use,
            such as "fs" or "my_files"

        .. mongodoc:: gridfs

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
            collection)

    def wrap(self, obj):
        if obj.__class__ is grid_file.GridIn:
            grid_in_class = create_class_with_framework(
                MongoMotorAgnosticGridIn, self._framework, self.__module__)

            return grid_in_class(
                root_collection=self.collection,
                delegate=obj)

        elif obj.__class__ is grid_file.GridOut:
            grid_out_class = create_class_with_framework(
                MongoMotorAgnosticGridOut, self._framework, self.__module__)

            return grid_out_class(
                root_collection=self.collection,
                delegate=obj)

        elif obj.__class__ is gridfs.GridOutCursor:
            grid_out_class = create_class_with_framework(
                AgnosticGridOutCursor, self._framework, self.__module__)

            return grid_out_class(
                cursor=obj,
                collection=self.collection)
