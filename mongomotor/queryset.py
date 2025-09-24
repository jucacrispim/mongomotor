# -*- coding: utf-8 -*-

# Copyright 2016, 2025 Juca Crispim <juca@poraodojuca.dev>

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

from bson.code import Code
from bson import SON
import os
import re
from mongoengine import DENY, CASCADE, NULLIFY, PULL
from mongoengine.common import _import_class
from mongoengine.connection import get_db
from mongoengine.context_managers import (
    set_write_concern,
    set_read_write_concern,
)
from mongoengine.errors import (
    OperationError,
    BulkWriteError,
    NotUniqueError,
    LookUpError,
)
from mongoengine.queryset import transform
from mongoengine.queryset.queryset import QuerySet as MEQuerySet
import pymongo
from pymongo import ReturnDocument
from mongomotor import signals

# for tests
TEST_ENV = os.environ.get('MONGOMOTOR_TEST_ENV')


class QuerySet(MEQuerySet):

    def __repr__(self):  # pragma no cover
        return self.__class__.__name__

    def __len__(self):
        raise TypeError('len() is not supported. Use count()')

    def _iter_results(self):
        try:
            return super()._iter_results()
        except StopIteration:
            raise StopAsyncIteration

    def __getitem__(self, index):
        skip = self._skip or 0
        # If we received an slice we will return a queryset
        # and as we will not touch the db now we do not need a future
        # here
        if isinstance(index, slice):
            query = self.clone()
            query = query.skip(index.start + skip)
            query = query.limit(index.stop - index.start)
            return query

        else:
            query = self.clone()
            query = query.skip(index + skip).limit(1)
            return query.first()

    def __aiter__(self):
        return self

    async def __anext__(self):
        async for doc in self._cursor:
            mm_doc = self._document._from_son(
                doc,
                _auto_dereference=self._auto_dereference)
            return mm_doc
        else:
            raise StopAsyncIteration()

    async def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~mongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.
        """

        queryset = self.clone()
        queryset = queryset.order_by().limit(2)
        queryset = queryset.filter(*q_objs, **query)

        docs = await queryset.to_list(length=2)
        if len(docs) < 1:
            msg = ("%s matching query does not exist."
                   % queryset._document._class_name)
            raise queryset._document.DoesNotExist(msg)

        elif len(docs) > 1:
            msg = 'More than 1 item returned'
            raise queryset._document.MultipleObjectsReturned(msg)

        return docs[0]

    async def first(self):
        """Retrieve the first object matching the query.
        """
        queryset = self.clone()
        result = await queryset.limit(1).to_list()
        try:
            return result[0]
        except IndexError:
            return None

    async def count(self, with_limit_and_skip=True):
        """Counts the documents in the queryset.

        :param with_limit_and_skip: Indicates if limit and skip applied to
          the queryset should be taken into account."""

        if self._limit == 0 and with_limit_and_skip or self._none:
            return 0

        kw = {}
        if with_limit_and_skip and self._limit:
            kw['limit'] = self._limit

        if with_limit_and_skip and self._skip:
            kw['skip'] = self._skip

        return await self._collection.count_documents(self._query, **kw)

    async def insert(
        self, doc_or_docs, load_bulk=True, write_concern=None,
        signal_kwargs=None
    ):
        """bulk insert documents

        :param doc_or_docs: a document or list of documents to be inserted
        :param load_bulk (optional): If True returns the list of document
            instances
        :param write_concern: Extra keyword arguments are passed down to
                :meth:`~pymongo.collection.Collection.insert`
                which will be used as options for the resultant
                ``getLastError`` command.  For example,
                ``insert(..., {w: 2, fsync: True})`` will wait until at least
                two servers have recorded the write and will force an fsync on
                each server being written to.
        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        By default returns document instances, set ``load_bulk`` to False to
        return just ``ObjectIds``
        """
        Document = _import_class("Document")

        if write_concern is None:
            write_concern = {}

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, Document) or issubclass(docs.__class__, Document):
            return_one = True
            docs = [docs]

        for doc in docs:
            if not isinstance(doc, self._document):
                msg = "Some documents inserted aren't instances of %s" % str(
                    self._document
                )
                raise OperationError(msg)
            if doc.pk and not doc._created:
                msg = "Some documents have ObjectIds, use doc.update() instead"
                raise OperationError(msg)

        signal_kwargs = signal_kwargs or {}
        signals.pre_bulk_insert.send(
            self._document, documents=docs, **signal_kwargs)

        raw = [doc.to_mongo() for doc in docs]

        with set_write_concern(self._collection, write_concern) as collection:
            insert_func = collection.insert_many
            if return_one:
                raw = raw[0]
                insert_func = collection.insert_one

        try:
            inserted_result = await insert_func(raw)
            ids = (
                [inserted_result.inserted_id]
                if return_one
                else inserted_result.inserted_ids
            )
        except pymongo.errors.DuplicateKeyError as err:
            message = "Could not save document (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.BulkWriteError as err:
            # inserting documents that already have an _id field will
            # give huge performance debt or raise
            message = "Bulk write error: (%s)"
            raise BulkWriteError(message % err.details)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Apply inserted_ids to documents
        for doc, doc_id in zip(docs, ids):
            doc.pk = doc_id

        if not load_bulk:
            signals.post_bulk_insert.send(
                self._document, documents=docs, loaded=False, **signal_kwargs
            )
            return ids[0] if return_one else ids

        documents = await self.in_bulk(ids)
        results = [documents.get(obj_id) for obj_id in ids]
        signals.post_bulk_insert.send(
            self._document, documents=results, loaded=True, **signal_kwargs
        )
        return results[0] if return_one else results

    async def update(
        self,
        upsert=False,
        multi=True,
        write_concern=None,
        read_concern=None,
        full_result=False,
        array_filters=None,
        **update,
    ):
        """Perform an atomic update on the fields matched by the query.

        :param upsert: insert if document doesn't exist (default ``False``)
        :param multi: Update multiple documents.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param read_concern: Override the read concern for the operation
        :param full_result: Return the associated ``pymongo.UpdateResult``
            rather than just the number updated items
        :param array_filters: A list of filters specifying which array elements
            an update should apply.
        :param update: Django-style update keyword arguments

        :returns the number of updated documents (unless ``full_result``
            is True)
        """
        if not update and not upsert:
            raise OperationError("No update parameters, would remove data")

        if write_concern is None:
            write_concern = {}
        if self._none or self._empty:
            return 0

        queryset = self.clone()
        query = queryset._query
        if "__raw__" in update and isinstance(
            update["__raw__"], list
        ):  # Case of Update with Aggregation Pipeline
            update = [
                transform.update(queryset._document, **{"__raw__": u})
                for u in update["__raw__"]
            ]
        else:
            update = transform.update(queryset._document, **update)
        # If doing an atomic upsert on an inheritable class
        # then ensure we add _cls to the update operation
        if upsert and "_cls" in query:
            if "$set" in update:
                update["$set"]["_cls"] = queryset._document._class_name
            else:
                update["$set"] = {"_cls": queryset._document._class_name}
        try:
            with set_read_write_concern(
                queryset._collection, write_concern, read_concern
            ) as collection:
                update_func = collection.update_one
                if multi:
                    update_func = collection.update_many
                result = await update_func(
                    query, update, upsert=upsert, array_filters=array_filters
                )
            if full_result:
                return result
            elif result.raw_result:
                return result.raw_result["n"]
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError("Update failed (%s)" % err)
        except pymongo.errors.OperationFailure as err:
            if str(err) == "multi not coded yet":
                message = "update() method requires MongoDB 1.1.3+"
                raise OperationError(message)
            raise OperationError("Update failed (%s)" % err)

    async def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ObjectId's
        :rtype: dict of ObjectId's as keys and collection-specific
                Document subclasses as values.
        """
        doc_map = {}

        docs = self._collection.find(
            {"_id": {"$in": object_ids}}, **self._cursor_args)
        if self._scalar:
            async for doc in docs:
                doc_map[doc["_id"]] = self._get_scalar(
                    self._document._from_son(doc))
        elif self._as_pymongo:
            async for doc in docs:
                doc_map[doc["_id"]] = doc
        else:
            async for doc in docs:
                doc_map[doc["_id"]] = self._document._from_son(
                    doc,
                    _auto_dereference=self._auto_dereference,
                )

        return doc_map

    async def delete(self, write_concern=None, _from_doc_delete=False,
                     cascade_refs=None):
        """Deletes the documents matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param _from_doc_delete: True when called from document delete
          therefore signals will have been triggered so don't loop.

        :returns number of deleted documents
        """
        queryset = self.clone()
        doc = queryset._document

        if write_concern is None:
            write_concern = {}

        # Handle deletes where skips or limits have been applied or
        # there is an untriggered delete signal
        has_delete_signal = (
            signals.pre_delete.has_receivers_for(self._document) or
            signals.post_delete.has_receivers_for(self._document))

        call_document_delete = (queryset._skip or queryset._limit or
                                has_delete_signal) and not _from_doc_delete

        if call_document_delete:
            r = await self._document_delete(queryset, write_concern)
            return r

        await self._check_delete_rules(doc, queryset, cascade_refs,
                                       write_concern)

        r = await queryset._collection.delete_many(
            queryset._query, **write_concern)

        return r

    async def upsert_one(self, write_concern=None, **update):
        """Overwrite or add the first document matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param update: Django-style update keyword arguments

        :returns the new or overwritten document

        """

        result = await self.update(multi=False, upsert=True,
                                   write_concern=write_concern,
                                   full_result=True, **update)
        result = result.raw_result
        if result['updatedExisting']:
            doc = await self.first()
        else:
            doc = await self._document.objects.with_id(
                result['upserted'])

        return doc

    async def to_list(self, length=100):
        """Returns a list of the current documents in the queryset.

        :param length: maximum number of documents to return for this call."""

        cursor = self._cursor
        docs_list = await cursor.to_list(length)

        final_list = [self._document._from_son(
            d, _auto_dereference=self._auto_dereference)
            for d in docs_list]

        return final_list

    async def item_frequencies(self, field, normalize=False):
        """Returns a dictionary of all items present in a field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds, or searching documents.

        .. note::

            Can only do direct simple mappings and cannot map across
            :class:`~mongomotor.fields.ReferenceField` or
            :class:`~mongomotor.fields.GenericReferenceField` for more complex
            counting a manual aggretation call would be required.

        If the field is a :class:`~mongomotor.fields.ListField`,
        the items within each list will be counted individually.

        :param field: the field to use
        :param normalize: normalize the results so they add to 1.0
        """

        cursor = await self._document._get_collection().aggregate([
            {'$match': self._query},
            {'$unwind': f'${field}'},
            {'$group': {'_id': '$' + field, 'total': {'$sum': 1}}}
        ])
        freqs = {}
        async for doc in cursor:
            freqs[doc['_id']] = doc['total']

        if normalize:
            count = sum(freqs.values())
            freqs = dict([(k, float(v) / count)
                          for k, v in list(freqs.items())])

        return freqs

    async def average(self, field):
        """Average over the values of the specified field.

        :param field: the field to average over; use dot-notation to refer to
            embedded document fields

        This method is more performant than the regular `average`, because it
        uses the aggregation framework instead of map-reduce.
        """
        cursor = await self._document._get_collection().aggregate([
            {'$match': self._query},
            {'$group': {'_id': 'avg', 'total': {'$avg': '$' + field}}}
        ])

        avg = 0
        async for doc in cursor:
            avg = doc['total']
            break

        return avg

    async def aggregate(self, pipeline, **kwargs):
        """Perform an aggregate function based on your queryset params

        :param pipeline: list of aggregation commands,
            see: https://www.mongodb.com/docs/manual/core/aggregation-pipeline/
        :param kwargs: (optional) kwargs dictionary to be passed to pymongo's
            aggregate call.
        """
        user_pipeline = pipeline

        initial_pipeline = []
        if self._none or self._empty:
            initial_pipeline.append({"$limit": 1})
            initial_pipeline.append({"$match": {"$expr": False}})

        if self._query:
            initial_pipeline.append({"$match": self._query})

        if self._ordering:
            initial_pipeline.append({"$sort": dict(self._ordering)})

        if self._limit is not None:
            # As per MongoDB Documentation
            # (https://www.mongodb.com/docs/manual/reference/operator/aggregation/limit/),
            # keeping limit stage right after sort stage is more efficient.
            # But this leads to wrong set of documents
            # for a skip stage that might succeed these. So we need to maintain
            # more documents in memory in such a case
            # (https://stackoverflow.com/a/24161461).
            initial_pipeline.append(
                {"$limit": self._limit + (self._skip or 0)})

        if self._skip is not None:
            initial_pipeline.append({"$skip": self._skip})

        final_pipeline = initial_pipeline + user_pipeline

        collection = self._collection
        if self._read_preference is not None or self._read_concern is not None:
            collection = self._collection.with_options(
                read_preference=self._read_preference,
                read_concern=self._read_concern
            )

        return await collection.aggregate(final_pipeline, cursor={}, **kwargs)

    async def map_reduce(
        self, map_f, reduce_f, output, finalize_f=None, limit=None, scope=None
    ):
        """Perform a map/reduce query using the current query spec
        and ordering. While ``map_reduce`` respects ``QuerySet`` chaining,
        it must be the last call made, as it does not return a maleable
        ``QuerySet``.

        See the :meth:`~mongoengine.tests.QuerySetTest.test_map_reduce`
        and :meth:`~mongoengine.tests.QuerySetTest.test_map_advanced`
        tests in ``tests.queryset.QuerySetTest`` for usage examples.

        :param map_f: map function, as :class:`~bson.code.Code` or string
        :param reduce_f: reduce function, as
                         :class:`~bson.code.Code` or string
        :param output: output collection name, if set to 'inline' will return
           the results inline. This can also be a dictionary containing output
           options see:
           https://www.mongodb.com/docs/manual/reference/command/mapReduce/#mongodb-dbcommand-dbcmd.mapReduce
        :param finalize_f: finalize function, an optional function that
                           performs any post-reduction processing.
        :param scope: values to insert into map/reduce global scope. Optional.
        :param limit: number of objects from current query to provide
                      to map/reduce method

        Returns an iterator yielding
        :class:`~mongoengine.document.MapReduceDocument`.
        """
        queryset = self.clone()

        MapReduceDocument = _import_class("MapReduceDocument")

        map_f_scope = {}
        if isinstance(map_f, Code):
            map_f_scope = map_f.scope
            map_f = str(map_f)
        map_f = Code(queryset._sub_js_fields(map_f), map_f_scope or None)

        reduce_f_scope = {}
        if isinstance(reduce_f, Code):
            reduce_f_scope = reduce_f.scope
            reduce_f = str(reduce_f)
        reduce_f_code = queryset._sub_js_fields(reduce_f)
        reduce_f = Code(reduce_f_code, reduce_f_scope or None)

        mr_args, inline = self._get_map_reduce_args(
            queryset, finalize_f, scope, limit, output)

        db = queryset._document._get_db()
        result = await db.command(
            {
                "mapReduce": queryset._document._get_collection_name(),
                "map": map_f,
                "reduce": reduce_f,
                **mr_args,
            }
        )

        if inline:
            docs = result["results"]
        else:
            if isinstance(result["result"], str):
                docs = await db[result["result"]].find().to_list()
            else:
                info = result["result"]
                docs = await db.client[
                    info["db"]][info["collection"]].find().to_list()

        if queryset._ordering:
            docs = docs.sort(queryset._ordering)

        for doc in docs:
            yield MapReduceDocument(
                queryset._document, queryset._collection, doc["_id"],
                doc["value"]
            )

    async def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot-notation to refer to
            embedded document fields

        This method is more performant than the regular `sum`, because it uses
        the aggregation framework instead of map-reduce.
        """
        cursor = await self._document._get_collection().aggregate([
            {'$match': self._query},
            {'$group': {'_id': 'sum', 'total': {'$sum': '$' + field}}}
        ])

        r = 0
        async for doc in cursor:
            r = doc['total']
            break

        return r

    async def distinct(self, field):
        """Return a list of distinct values for a given field.

        :param field: the field to select distinct values from

        .. note:: This is a command and won't take ordering or limit into
           account.
        """
        queryset = self.clone()

        try:
            field = self._fields_to_dbfields([field]).pop()
        except LookUpError:
            pass

        raw_values = await queryset._cursor.distinct(field)
        if not self._auto_dereference:
            return raw_values

        distinct = await self._dereference(
            raw_values, 1, name=field, instance=self._document)

        doc_field = self._document._fields.get(field.split(".", 1)[0])
        instance = None

        # We may need to cast to the correct type eg.
        # ListField(EmbeddedDocumentField)
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        ListField = _import_class("ListField")
        GenericEmbeddedDocumentField = _import_class(
            "GenericEmbeddedDocumentField")
        if isinstance(doc_field, ListField):
            doc_field = getattr(doc_field, "field", doc_field)
        if isinstance(doc_field, (EmbeddedDocumentField,
                                  GenericEmbeddedDocumentField)):
            instance = getattr(doc_field, "document_type", None)

        # handle distinct on subdocuments
        if "." in field:
            for field_part in field.split(".")[1:]:
                # if looping on embedded document, get the document
                # type instance
                if instance and isinstance(
                    doc_field, (EmbeddedDocumentField,
                                GenericEmbeddedDocumentField)
                ):
                    doc_field = instance
                # now get the subdocument
                doc_field = getattr(doc_field, field_part, doc_field)
                # We may need to cast to the correct type eg.
                # ListField(EmbeddedDocumentField)
                if isinstance(doc_field, ListField):
                    doc_field = getattr(doc_field, "field", doc_field)
                if isinstance(
                    doc_field, (EmbeddedDocumentField,
                                GenericEmbeddedDocumentField)
                ):
                    instance = getattr(doc_field, "document_type", None)

        if instance and isinstance(
            doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)
        ):
            distinct = [instance(**doc) for doc in distinct]

        return distinct

    async def modify(
        self,
        upsert=False,
        remove=False,
        new=False,
        array_filters=None,
        **update,
    ):
        """Update and return the updated document.

        Returns either the document before or after modification based on `new`
        parameter. If no documents match the query and `upsert` is false,
        returns ``None``. If upserting and `new` is false, returns ``None``.


        :param upsert: insert if document doesn't exist (default ``False``)
        :param full_response: return the entire response object from the
            server (default ``False``, not available for PyMongo 3+)
        :param remove: remove rather than updating (default ``False``)
        :param new: return updated rather than original document
            (default ``False``)
        :param array_filters: A list of filters specifying which array
            elements an update should apply.
        :param update: Django-style update keyword arguments
        """

        if remove and new:
            raise OperationError("Conflicting parameters: remove and new")

        if not update and not upsert and not remove:
            raise OperationError(
                "No update parameters, must either update or remove")

        if self._none or self._empty:
            return None

        queryset = self.clone()
        query = queryset._query
        if not remove:
            update = transform.update(queryset._document, **update)
        sort = queryset._ordering

        try:
            if remove:
                result = await queryset._collection.find_one_and_delete(
                    query, sort=sort, **self._cursor_args
                )
            else:
                if new:
                    return_doc = ReturnDocument.AFTER
                else:
                    return_doc = ReturnDocument.BEFORE
                result = await queryset._collection.find_one_and_update(
                    query,
                    update,
                    upsert=upsert,
                    sort=sort,
                    return_document=return_doc,
                    array_filters=array_filters,
                    **self._cursor_args,
                )
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError("Update failed (%s)" % err)
        except pymongo.errors.OperationFailure as err:
            raise OperationError("Update failed (%s)" % err)

        if result is not None:
            result = self._document._from_son(result)

        return result

    async def explain(self):
        """Return an explain plan record for the
        :class:`~mongoengine.queryset.QuerySet` cursor.
        """
        return await self._cursor.explain()

    @property
    def fetch_next(self):
        return self._cursor.fetch_next

    def next_object(self):
        raw = self._cursor.next_object()
        return self._document._from_son(
            raw, _auto_dereference=self._auto_dereference)

    def no_cache(self):
        """Convert to a non-caching queryset
        """
        if self._result_cache is not None:
            raise OperationError('QuerySet already cached')

        return self._clone_into(QuerySetNoCache(self._document,
                                                self._collection))

    def _get_code(self, func):
        f_scope = {}
        if isinstance(func, Code):
            f_scope = func.scope
            func = str(func)
        func = Code(self._sub_js_fields(func), f_scope)
        return func

    async def _check_delete_rules(self, doc, queryset, cascade_refs,
                                  write_concern):
        """Checks the delete rules for documents being deleted in a queryset.
        Raises an exception if any document has a DENY rule."""

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
        delete_rules = delete_rules.copy()
        fields = [d async for d in self]
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get('abstract'):
                continue
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == DENY and document_cls.objects(
                    **{field_name + '__in': self}).count() > 0:
                msg = ("Could not delete document (%s.%s refers to it)"
                       % (document_cls.__name__, field_name))
                raise OperationError(msg)

        if not delete_rules:
            return

        r = None
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get('abstract'):
                continue
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == CASCADE:
                cascade_refs = set() if cascade_refs is None else cascade_refs
                async for ref in queryset:
                    cascade_refs.add(ref.id)

                ref_q = document_cls.objects(**{field_name + '__in': fields,
                                                'id__nin': cascade_refs})

                count = await ref_q.count()
                if count > 0:
                    r = await ref_q.delete(write_concern=write_concern,
                                           cascade_refs=cascade_refs)

            elif rule in (NULLIFY, PULL):
                if rule == NULLIFY:
                    updatekw = {'unset__%s' % field_name: 1}
                else:
                    updatekw = {'pull_all__%s' % field_name: fields}

                r = await document_cls.objects(
                    **{field_name + '__in': fields}).update(
                        write_concern=write_concern, **updatekw)

        return r

    async def _document_delete(self, queryset, write_concern):
        """Delete the documents in queryset by calling the document's delete
        method."""

        cnt = 0
        async for doc in queryset:
            await doc.delete(**write_concern)
            cnt += 1
        return cnt

    def _get_map_reduce_args(self, queryset, finalize_f, scope, limit, output):
        mr_args = {"query": queryset._query}

        if finalize_f:
            finalize_f_scope = {}
            if isinstance(finalize_f, Code):
                finalize_f_scope = finalize_f.scope
                finalize_f = str(finalize_f)
            finalize_f_code = queryset._sub_js_fields(finalize_f)
            finalize_f = Code(finalize_f_code, finalize_f_scope or None)
            mr_args["finalize"] = finalize_f

        if scope:
            mr_args["scope"] = scope

        if limit:
            mr_args["limit"] = limit

        if output == "inline" and not queryset._ordering:
            inline = True
            mr_args["out"] = {"inline": 1}
        else:
            inline = False
            if isinstance(output, str):
                mr_args["out"] = output

            elif isinstance(output, dict):
                ordered_output = []

                for part in ("replace", "merge", "reduce"):
                    value = output.get(part)
                    if value:
                        ordered_output.append((part, value))
                        break

                else:
                    raise OperationError("actionData not specified for output")

                db_alias = output.get("db_alias")
                remaing_args = ["db", "sharded", "nonAtomic"]

                if db_alias:
                    ordered_output.append(("db", get_db(db_alias).name))
                    del remaing_args[0]

                for part in remaing_args:
                    value = output.get(part)
                    if value:
                        ordered_output.append((part, value))

                mr_args["out"] = SON(ordered_output)

        return mr_args, inline


class QuerySetNoCache(QuerySet):
    """A non caching QuerySet"""

    def cache(self):
        """Convert to a caching queryset
        """
        return self._clone_into(QuerySet(self._document, self._collection))

    def __iter__(self):
        queryset = self
        if queryset._iter:
            queryset = self.clone()
        queryset.rewind()
        return queryset
