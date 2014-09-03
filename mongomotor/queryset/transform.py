# -*- coding: utf-8 -*-

from collections import defaultdict
import tornado
from tornado import gen
from mongoengine.common import _import_class
from mongoengine.errors import InvalidQueryError
from mongoengine.queryset import transform


@gen.coroutine
def query(_doc_cls=None, _field_operation=False, **query):
    """Transform a query from Django-style format to Mongo format.
    """
    mongo_query = {}
    merge_query = defaultdict(list)
    for key, value in sorted(query.items()):
        if key == "__raw__":
            mongo_query.update(value)
            continue

        parts = key.split('__')
        indices = [(i, p) for i, p in enumerate(parts) if p.isdigit()]
        parts = [part for part in parts if not part.isdigit()]
        # Check for an operator and transform to mongo-style if there is
        op = None
        if len(parts) > 1 and parts[-1] in transform.MATCH_OPERATORS:
            op = parts.pop()

        negate = False
        if len(parts) > 1 and parts[-1] == 'not':
            parts.pop()
            negate = True

        if _doc_cls:
            # Switch field names to proper names [set in Field(name='foo')]
            try:
                fields = _doc_cls._lookup_field(parts)
            except Exception as e:
                raise InvalidQueryError(e)
            parts = []

            cleaned_fields = []
            for field in fields:
                append_field = True
                if isinstance(field, str):
                    parts.append(field)
                    append_field = False
                else:
                    parts.append(field.db_field)
                if append_field:
                    cleaned_fields.append(field)

            # Convert value to proper value
            field = cleaned_fields[-1]

            singular_ops = [None, 'ne', 'gt', 'gte', 'lt', 'lte', 'not']
            singular_ops += transform.STRING_OPERATORS
            if op in singular_ops:
                if isinstance(field, str):
                    if (op in transform.STRING_OPERATORS and
                       isinstance(value, str)):
                        StringField = _import_class('StringField')
                        value = StringField.prepare_query_value(op, value)
                    else:
                        value = field
                else:
                    if isinstance(value, tornado.concurrent.Future):
                        value = yield value

                    value = field.prepare_query_value(op, value)

            elif op in ('in', 'nin', 'all', 'near') and not isinstance(
                    value, dict):
                # 'in', 'nin' and 'all' require a list of values
                if not isinstance(value, list):
                    vlist = yield value.to_list()
                else:
                    vlist = value

                value = [field.prepare_query_value(op, v) for v in vlist]

        # if op and op not in COMPARISON_OPERATORS:
        if op:
            if op in transform.GEO_OPERATORS:
                value = _geo_operator(field, op, value)
            elif op in transform.CUSTOM_OPERATORS:
                if op == 'match':
                    value = field.prepare_query_value(op, value)
                    value = {"$elemMatch": value}
                else:
                    NotImplementedError("Custom method '%s' has not "
                                        "been implemented" % op)
            elif op not in transform.STRING_OPERATORS:
                value = {'$' + op: value}

        if negate:
            value = {'$not': value}

        for i, part in indices:
            parts.insert(i, part)
        key = '.'.join(parts)
        if op is None or key not in mongo_query:
            mongo_query[key] = value
        elif key in mongo_query:
            if key in mongo_query and isinstance(mongo_query[key], dict):
                mongo_query[key].update(value)
                # $maxDistance needs to come last - convert to SON
                if '$maxDistance' in mongo_query[key]:
                    value_dict = mongo_query[key]
                    value_son = SON()
                    for k, v in value_dict.items():
                        if k == '$maxDistance':
                            continue
                        value_son[k] = v
                    value_son['$maxDistance'] = value_dict['$maxDistance']
                    mongo_query[key] = value_son
            else:
                # Store for manually merging later
                merge_query[key].append(value)

    # The queryset has been filter in such a way we must manually merge
    for k, v in list(merge_query.items()):
        merge_query[k].append(mongo_query[k])
        del mongo_query[k]
        if isinstance(v, list):
            value = [{k: val} for val in v]
            if '$and' in list(mongo_query.keys()):
                mongo_query['$and'].append(value)
            else:
                mongo_query['$and'] = value

    return mongo_query
