=======================
 Querying the database
=======================
:class:`~mongomotor.Document` classes have an :attr:`objects` attribute, which
is used for accessing the objects in the database associated with the class.
The :attr:`objects` attribute is actually a
:class:`mongoengine.queryset.QuerySetManager`, which creates and returns a new
:class:`~mongomotor.queryset.QuerySet` object on access. The
:class:`~mongomotor.queryset.QuerySet` object may be iterated over to
fetch documents from the database:

.. code-block:: python

    # Prints out the names of all the artists in the database
    async for artist in Aritst.objects:
        print(artist.name)


.. note::

    MongoMotor querysets utilise a local cache.  So iterating
    it multiple times will only cause a single query.  If this is not the
    desired behaviour you can call
    :class:`~mongomotor.queryset.QuerySet.no_cache` to return a non-caching
    queryset.


Filtering queries
=================
The query may be filtered by calling the
:class:`~mongomotor.queryset.QuerySet` object with field lookup keyword
arguments. The keys in the keyword arguments correspond to fields on the
:class:`~mongomotor.Document` you are querying:

.. code-block:: python

    # This will return a QuerySet that will only iterate over albums
    # with the (very uncommon...) title 'The best of'
    albums = Album.objects(title='The best of')

Fields on embedded documents may also be referred to using field lookup syntax
by using a double-underscore in place of the dot in object attribute access
syntax:

.. code-block:: python

    # This will return a QuerySet that will only iterate over albums that have
    # that have a track titled 'Tormentor'
    albums = Album.objects(track__title='Tormentor')

.. note::

   if your field name is like mongodb operator name (for example
   type, lte, lt...) and you want to place it at the end of lookup keyword
   mongoengine automatically  prepend $ to it. To avoid this use  __ at the end
   of your lookup keyword. For example if your field name is ``type`` and you
   want to query by this field you must use ``.objects(track__type__="cover")``
   instead of ``.objects(track__type="cover")``

Query operators
===============
Operators other than equality may also be used in queries --- just attach the
operator name to a key with a double-underscore:

.. code-block:: python

    # Only find albums with rate 4 or more
    good_albums = Users.objects(rating__gte=4)

Available operators are as follows:

* ``ne`` -- not equal to
* ``lt`` -- less than
* ``lte`` -- less than or equal to
* ``gt`` -- greater than
* ``gte`` -- greater than or equal to
* ``not`` -- negate a standard check, may be used before other operators (e.g.
  ``Q(title__not__in=['Tormentor', 'Death trap'])``)
* ``in`` -- value is in list (a list of values should be provided)
* ``nin`` -- value is not in list (a list of values should be provided)
* ``mod`` -- ``value % x == y``, where ``x`` and ``y`` are two provided values
* ``all`` -- every item in list of values provided is in array
* ``size`` -- the size of the array is
* ``exists`` -- value for field exists

String queries
--------------

The following operators are available as shortcuts to querying with regular
expressions:

* ``exact`` -- string field exactly matches value
* ``iexact`` -- string field exactly matches value (case insensitive)
* ``contains`` -- string field contains value
* ``icontains`` -- string field contains value (case insensitive)
* ``startswith`` -- string field starts with value
* ``istartswith`` -- string field starts with value (case insensitive)
* ``endswith`` -- string field ends with value
* ``iendswith`` -- string field ends with value (case insensitive)
* ``match``  -- performs an $elemMatch so you can match an entire document within an array


Geo queries
-----------

There are a few special operators for performing geographical queries.
The following were added in MongoEngine 0.8 for
:class:`mongoengine.fields.PointField`,
:class:`mongoengine.fields.LineStringField` and
:class:`mongoengine.fields.PolygonField`:

* ``geo_within`` -- check if a geometry is within a polygon. For ease of use
  it accepts either a geojson geometry or just the polygon coordinates eg::

        loc.objects(point__geo_within=[[[40, 5], [40, 6], [41, 6], [40, 5]]])
        loc.objects(point__geo_within={"type": "Polygon",
                                       "coordinates": [[[40, 5], [40, 6],
				                        [41, 6], [40, 5]]]})

* ``geo_within_box`` -- simplified geo_within searching with a box eg::

        loc.objects(point__geo_within_box=[(-125.0, 35.0), (-100.0, 40.0)])
        loc.objects(point__geo_within_box=[<bottom left coordinates>,
	                                   <upper right coordinates>])

* ``geo_within_polygon`` -- simplified geo_within searching within a simple
  polygon eg::

        loc.objects(point__geo_within_polygon=[[40, 5], [40, 6], [41, 6],
	                                       [40, 5]])
        loc.objects(point__geo_within_polygon=[ [ <x1> , <y1> ] ,
                                                [ <x2> , <y2> ] ,
                                                [ <x3> , <y3> ] ])

* ``geo_within_center`` -- simplified geo_within the flat circle radius of a
  point eg::

        loc.objects(point__geo_within_center=[(-125.0, 35.0), 1])
        loc.objects(point__geo_within_center=[ [ <x>, <y> ] , <radius> ])

* ``geo_within_sphere`` -- simplified geo_within the spherical circle radius of
  a point eg::

        loc.objects(point__geo_within_sphere=[(-125.0, 35.0), 1])
        loc.objects(point__geo_within_sphere=[ [ <x>, <y> ] , <radius> ])

* ``geo_intersects`` -- selects all locations that intersect with a geometry
  eg::

        # Inferred from provided points lists:
        loc.objects(poly__geo_intersects=[40, 6])
        loc.objects(poly__geo_intersects=[[40, 5], [40, 6]])
        loc.objects(poly__geo_intersects=[[[40, 5], [40, 6], [41, 6], [41, 5],
	                                 [40, 5]]])

        # With geoJson style objects
        loc.objects(poly__geo_intersects={"type": "Point",
	                                  "coordinates": [40, 6]})
        loc.objects(poly__geo_intersects={"type": "LineString",
                                          "coordinates": [[40, 5], [40, 6]]})
        loc.objects(poly__geo_intersects={"type": "Polygon",
                                          "coordinates": [[[40, 5], [40, 6],
					                    [41, 6], [41, 5],
							    [40, 5]]]})

* ``near`` -- find all the locations near a given point::

        loc.objects(point__near=[40, 5])
        loc.objects(point__near={"type": "Point", "coordinates": [40, 5]})

  You can also set the maximum and/or the minimum distance in meters as well::

        loc.objects(point__near=[40, 5], point__max_distance=1000)
        loc.objects(point__near=[40, 5], point__min_distance=100)

The older 2D indexes are still supported with the
:class:`mongoengine.fields.GeoPointField`:

* ``within_distance`` -- provide a list containing a point and a maximum
  distance (e.g. [(41.342, -87.653), 5])
* ``within_spherical_distance`` -- same as above but using the spherical geo model
  (e.g. [(41.342, -87.653), 5/earth_radius])
* ``near`` -- order the documents by how close they are to a given point
* ``near_sphere`` -- Same as above but using the spherical geo model
* ``within_box`` -- filter documents to those within a given bounding box (e.g.
  [(35.0, -125.0), (40.0, -100.0)])
* ``within_polygon`` -- filter documents to those within a given polygon (e.g.
  [(41.91,-87.69), (41.92,-87.68), (41.91,-87.65), (41.89,-87.65)]).

  .. note:: Requires Mongo Server 2.0

* ``max_distance`` -- can be added to your location queries to set a maximum
  distance.
* ``min_distance`` -- can be added to your location queries to set a minimum
  distance.

Querying lists
--------------
On most fields, this syntax will look up documents where the field specified
matches the given value exactly, but when the field refers to a
:class:`mongoengine.fields.ListField`, a single item may be provided, in which case
lists that contain that item will be matched::

    class Page(Document):
        tags = ListField(StringField())

    # This will match all pages that have the word 'coding' as an item in the
    # 'tags' list
    Page.objects(tags='coding')

It is possible to query by position in a list by using a numerical value as a
query operator. So if you wanted to find all pages whose first tag was ``db``,
you could use the following query::

    Page.objects(tags__0='db')

If you only want to fetch part of a list eg: you want to paginate a list, then
the `slice` operator is required::

    # comments - skip 5, limit 10
    Page.objects.fields(slice__comments=[5, 10])

For updating documents, if you don't know the position in a list, you can use
the $ positional operator ::

    await Post.objects(comments__by="joe").update(
        **{'inc__comments__$__votes': 1})

However, this doesn't map well to the syntax so you can also use a capital S
instead ::

    await Post.objects(comments__by="joe").update(
        inc__comments__S__votes=1)

.. note::
    Due to :program:`Mongo`, currently the $ operator only applies to the
    first matched item in the query.


Raw queries
-----------
It is possible to provide a raw :mod:`PyMongo` query as a query parameter,
which will be integrated directly into the query. This is done using
the ``__raw__`` keyword argument::

    Page.objects(__raw__={'tags': 'coding'})


Limiting and skipping results
=============================
Just as with traditional ORMs, you may limit the number of results returned or
skip a number or results in you query.
:meth:`~mongomotor.queryset.QuerySet.limit` and
:meth:`~mongomotor.queryset.QuerySet.skip` and methods are available on
:class:`~mongomotor.queryset.QuerySet` objects, but the `array-slicing` syntax
is preferred for achieving this::

    # Only the first 5 people
    users = User.objects[:5]

    # All except for the first 5 people
    users = User.objects[5:]

    # 5 users, starting from the 10th user found
    users = User.objects[10:15]

You may also index the query to retrieve a single result. In this case it will
return a future. If an item at that index does not exists, an
:class:`IndexError` will be raised. A shortcut for retrieving the first result
and returning :attr:`None` if no result exists is provided
(:meth:`~mongomotor.queryset.QuerySet.first`)::

    >>> @asyncio.coroutine
    ... def do_stuff():
    ...     # Make sure there are no users
    ...     await User.drop_collection()
    ...     await User.objects.first() == None
    ...     await  User.objects[0]
    ...
    >>> loop.run_until_complete(do_stuff())
    IndexError: list index out of range
    >>>
    >>> @asyncio.coroutine
    ... def do_other_stuff():
    ...     await User(name='Test User').save()
    ...     await User.objects[0] == User.objects.first()
    ...
    >>> loop.run_until_complete(do_other_stuff())
    True


Retrieving unique results
-------------------------
To retrieve a result that should be unique in the collection, use
:meth:`~mongomotor.queryset.QuerySet.get`. This will raise
:class:`mongoengine.queryset.DoesNotExist` if
no document matches the query, and
:class:`mongoengine.queryset.MultipleObjectsReturned`
if more than one document matched the query.  These exceptions are merged into
your document definitions eg: `MyDoc.DoesNotExist`


Default Document queries
========================
By default, the objects :attr:`~Document.objects` attribute on a
document returns a :class:`~mongomotor.queryset.QuerySet` that doesn't filter
the collection -- it returns all objects. This may be changed by defining a
method on a document that modifies a queryset. The method should accept two
arguments -- :attr:`doc_cls` and :attr:`queryset`. The first argument is the
:class:`~mongomotor.Document` class that the method is defined on (in this
sense, the method is more like a :func:`classmethod` than a regular method),
and the second argument is the initial queryset. The method needs to be
decorated with :func:`mongoengine.queryset.queryset_manager` in order for it
to be recognised. ::

    class BlogPost(Document):
        title = StringField()
        date = DateTimeField()

        @queryset_manager
        def objects(doc_cls, queryset):
            # This may actually also be done by defining a default ordering for
            # the document, but this illustrates the use of manager methods
            return queryset.order_by('-date')

You don't need to call your method :attr:`objects` -- you may define as many
custom manager methods as you like::

    class BlogPost(Document):
        title = StringField()
        published = BooleanField()

        @queryset_manager
        def live_posts(doc_cls, queryset):
            return queryset.filter(published=True)

    await BlogPost(title='test1', published=False).save()
    await BlogPost(title='test2', published=True).save()
    assert (await BlogPost.objects.count()) == 2
    assert (await BlogPost.objects.count()) == 1

Custom QuerySets
================
Should you want to add custom methods for interacting with or filtering
documents, extending the :class:`~mongomotor.queryset.QuerySet` class may be
the way to go. To use a custom :class:`~mongomotor.queryset.QuerySet` class on
a document, set ``queryset_class`` to the custom class in a
:class:`~mongomotor.Document`'s ``meta`` dictionary::

    class AwesomerQuerySet(QuerySet):

        def get_awesome(self):
            return self.filter(awesome=True)

    class Page(Document):
        meta = {'queryset_class': AwesomerQuerySet}

    # To call:
    Page.objects.get_awesome()


Aggregation
===========
MongoDB provides some aggregation methods out of the box, but there are not as
many as you typically get with an RDBMS. MongoMotor provides a wrapper around
the built-in methods and provides some of its own (in fact, everything borrowed
from *MongoEngine*).

Basic Aggregation
-----------------

MongoMotor provides a wrapper around the built-in methods and provides
some of its own (borrowed from *MongoEngine* in fact). They are:
:meth:`~mongomotor.queryset.QuerySet.count`,
:meth:`~mongomotor.queryset.QuerySet.item_frequencies`,
:meth:`~mongomotor.queryset.QuerySet.average` and
:meth:`~mongomotor.queryset.QuerySet.sum`.

.. code-block:: python

   # the behavior of `count()` here  is different from mongoengine.
   # by default it takes into account skip and limit for the queryset.
   await Artist.objects.filter(name__ne='Toxic Holocaust').count()
   # to count the whole queryset set paramenter `with_skip_and_limit` to False.
   await Artist.objects.filter(name__ne='Toxic Holocaust').count(
       with_skip_and_limit=False)

   # to sum a specific field use:
   await Albums.objects.sum('times_played')

   # and for the average (mean):
   await Albums.objects.average('rating')

As MongoDB provides native lists, MongoMotor provides a helper method to get a
dictionary of the frequencies of items in lists across an entire collection --
:meth:`~mongomotor.queryset.QuerySet.item_frequencies`. An example of its use
would be generating "tag-clouds":

.. code-block:: python

    class Article(Document):
        tag = ListField(StringField())

    # After adding some tagged articles...
    tag_freqs = await Article.objects.item_frequencies('tag', normalize=True)

    from operator import itemgetter
    top_tags = sorted(tag_freqs.items(), key=itemgetter(1), reverse=True)[:10]


Further aggregation
-------------------

For further aggregation you may use
:meth:`~mongomotor.queryset.QuerySet.aggregate`. The `match` stage consists
in the filters used in the queryset. The others stages are passed as a list
to :meth:`~mongomotor.queryset.QuerySet.aggregate`.

.. code-block:: python

   # We'll count how many albums artists have.
   # Here the pipeline with the stages, except $match
   pipeline = [{'$unwind': '$artists'}
               {'$group': {'_id': '$artists', 'total': {'$sum': 1}}}]
   aggregation = Albums.objects.aggregate(pipeline)
   # The return of :meth:`~mongomotor.queryset.QuerySet.aggregate` is a
   # :meth:`~mongomotor.core.MongoMotorCursor` and we can iterate over it
   async for doc in aggregation:
       print('Artist: {} has {} albums'.format(doc._id, doc.total))

   # If we want to have a $match stage just filter the queryset
   aggregation = Albums.objects(track__title='Tormentor').aggregate(pipeline)


For more information on aggregation and map-reduce see
`MongoDB aggregation manual <https://docs.mongodb.com/manual/aggregation/>`_.


Query efficiency and performance
================================

There are a couple of methods to improve efficiency when querying, reducing the
information returned by the query or efficient dereferencing .

Retrieving a subset of fields
-----------------------------

Sometimes a subset of fields on a :class:`~mongomotor.Document` is required,
and for efficiency only these should be retrieved from the database. This issue
is especially important for MongoDB, as fields may often be extremely large
(e.g. a :class:`~mongomotor.fields.ListField` of
:class:`mongoengine.EmbeddedDocument`\ s, which represent the comments on a
blog post. To select only a subset of fields, use
:meth:`~mongomotor.queryset.QuerySet.only`, specifying the fields you want to
retrieve as its arguments. Note that if fields that are not downloaded are
accessed, their default value (or :attr:`None` if no default value is provided)
will be given:

.. code-block:: python

    >>> class Film(Document):
    ...     title = StringField()
    ...     year = IntField()
    ...     rating = IntField(default=3)
    ...
    >>> @asyncio.coroutine
    ... def retrive_only():
    ...     f = Film(title='The Shawshank Redemption', year=1994, rating=5)
    ...     await film.save()
    ...     f = await Film.objects.only('title').first()
    ...     print('title: {}'.format(f.title))
    ...     print('year: {}'.format(f.year)) # None
    ...     print('rating: {}'.format(f.year)) # default value
    ...
    >>> loop.run_until_complete(retrieve_only())
    title: The Shawshank Redemption
    year: None
    rating: 3

.. note::

    The :meth:`~mongomotor.queryset.QuerySet.exclude` is the opposite of
    :meth:`~mongomotor.queryset.QuerySet.only` if you want to exclude a field.

If you later need the missing fields, just call
:meth:`~mongomotor.Document.reload` on your document.

Getting related data
--------------------

When iterating the results of :class:`~mongomotor.fields.ListField` or
:class:`~mongomotor.fields.DictField` we automatically dereference any
:class:`~pymongo.dbref.DBRef` objects as efficiently as possible, reducing the
number the queries to mongo.

There are times when that efficiency is not enough, documents that have
:class:`~mongomotor.fields.ReferenceField` objects or
:class:`~mongomotor.fields.GenericReferenceField` objects at the top level are
expensive as the number of queries to MongoDB can quickly rise.

To limit the number of queries use
:func:`~mongomotor.queryset.QuerySet.select_related` which converts the
QuerySet to a list and dereferences as efficiently as possible.  By default
:func:`~mongomotor.queryset.QuerySet.select_related` only dereferences any
references to the depth of 1 level.  If you have more complicated documents and
want to dereference more of the object at once then increasing the :attr:`max_depth`
will dereference more levels of the document.

Turning off dereferencing
-------------------------

Sometimes for performance reasons you don't want to automatically dereference
data. To turn off dereferencing of the results of a query use
:func:`~mongomotor.queryset.QuerySet.no_dereference` on the queryset like so::

    post = await Post.objects.no_dereference().first()
    assert(isinstance(post.author, ObjectId))

You can also turn off all dereferencing for a fixed period by using the
:class:`mongoengine.context_managers.no_dereference` context manager::

    with no_dereference(Post) as Post:
        post = await Post.objects.first()
        assert(isinstance(post.author, ObjectId))

    # Outside the context manager dereferencing occurs.
    assert(isinstance(post.author, User))


Advanced queries
================

Sometimes calling a :class:`~mongomotor.queryset.QuerySet` object with keyword
arguments can't fully express the query you want to use -- for example if you
need to combine a number of constraints using *and* and *or*. This is made
possible in MongoEngine through the :class:`mongoengine.queryset.Q` class.
A :class:`mongoengine.queryset.Q` object represents part of a query, and
can be initialised using the same keyword-argument syntax you use to query
documents. To build a complex query, you may combine
:class:`mongoengine.queryset.Q` objects using the ``&`` (and) and ``|`` (or)
operators. To use a :class:`mongoengine.queryset.Q` object, pass it in as the
first positional argument to :attr:`Document.objects` when you filter it by
calling it with keyword arguments::

    # Get published posts
    Post.objects(Q(published=True) | Q(publish_date__lte=datetime.now()))

    # Get top posts
    Post.objects((Q(featured=True) & Q(hits__gte=1000)) | Q(hits__gte=5000))

.. warning:: You have to use bitwise operators.  You cannot use ``or``, ``and``
    to combine queries as ``Q(a=a) or Q(b=b)`` is not the same as
    ``Q(a=a) | Q(b=b)``. As ``Q(a=a)`` equates to true ``Q(a=a) or Q(b=b)`` is
    the same as ``Q(a=a)``.

.. _guide-atomic-updates:

Atomic updates
==============
Documents may be updated atomically by using the
:meth:`~mongomotor.queryset.QuerySet.update_one`,
:meth:`~mongomotor.queryset.QuerySet.update` and
:meth:`~mongomotor.queryset.QuerySet.modify` methods on a
:class:`~mongomotor.queryset.QuerySet` or
:meth:`~mongomotor.Document.modify` and
:meth:`~mongomotor.Document.save` (with :attr:`save_condition` argument) on a
:class:`~mongomotor.Document`.
There are several different "modifiers" that you may use with these methods:

* ``set`` -- set a particular value
* ``unset`` -- delete a particular value (since MongoDB v1.3)
* ``inc`` -- increment a value by a given amount
* ``dec`` -- decrement a value by a given amount
* ``push`` -- append a value to a list
* ``push_all`` -- append several values to a list
* ``pop`` -- remove the first or last element of a list `depending on the value`_
* ``pull`` -- remove a value from a list
* ``pull_all`` -- remove several values from a list
* ``add_to_set`` -- add value to a list only if its not in the list already

.. _depending on the value: http://docs.mongodb.org/manual/reference/operator/update/pop/

The syntax for atomic updates is similar to the querying syntax, but the
modifier comes before the field, not after it:

.. code-block:: python

    >>> @asyncio.coroutine
    ... def do_updates():
    ...     post = BlogPost(title='Test', page_views=0, tags=['database'])
    ...     await post.save()
    ...     await BlogPost.objects(id=post.id).update_one(inc__page_views=1)
    ...     await post.reload()
    ...     print('views: {}'.format(post.page_views))
    ...     await BlogPost.objects(id=post.id).update_one(set__title='Example Post')
    ...     await post.reload()
    ...     print('title {}'.format(post.title))
    ...     await BlogPost.objects(id=post.id).update_one(push__tags='nosql')
    ...     await post.reload()
    ...     print('tags {}'.format(','.join(post.tags)))
    ...
    >>> loop.run_until_complete(do_updates())
    views: 1
    title Example Post
    tags database, nosql

.. note::

    If no modifier operator is specified the default will be ``$set``. So the following sentences are identical:

.. code-block:: python

	await BlogPost.objects(id=post.id).update(title='Example Post')
        await BlogPost.objects(id=post.id).update(set__title='Example Post')

.. note::

    :meth:`~mongomotor.Document.save` runs atomic updates
    on changed documents by tracking changes to that document.

The positional operator allows you to update list items without knowing the
index position, therefore making the update a single atomic operation.  As we
cannot use the `$` syntax in keyword arguments it has been mapped to `S`:

.. code-block:: python

    post = BlogPost(title='Test', page_views=0, tags=['database', 'mongo'])
    await post.save()
    await BlogPost.objects(id=post.id).update(set__tags__S='mongodb')
    await post.reload()
    print(','.join(post.tags))
    database,mongodb

.. note::
    Currently only top level lists are handled, future versions of mongodb /
    pymongo plan to support nested positional operators.  See `The $ positional
    operator <http://www.mongodb.org/display/DOCS/Updating#Updating-The%24positionaloperator>`_.

Server-side javascript execution
================================
Javascript functions may be written and sent to the server for execution. The
result of this is the return value of the Javascript function. This
functionality is accessed through the
:meth:`~mongomotor.queryset.QuerySet.exec_js` method on
:meth:`~mongomotor.queryset.QuerySet` objects. Pass in a string containing a
Javascript function as the first argument.

The remaining positional arguments are names of fields that will be passed into
you Javascript function as its arguments. This allows functions to be written
that may be executed on any field in a collection (e.g. the
:meth:`~mongomotor.queryset.QuerySet.sum` method, which accepts the name of
the field to sum over as its argument). Note that field names passed in in this
manner are automatically translated to the names used on the database (set
using the :attr:`name` keyword argument to a field constructor).

Keyword arguments to :meth:`~mongomotor.queryset.QuerySet.exec_js` are
combined into an object called :attr:`options`, which is available in the
Javascript function. This may be used for defining specific parameters for your
function.

Some variables are made available in the scope of the Javascript function:

* ``collection`` -- the name of the collection that corresponds to the
  :class:`~mongomotor.Document` class that is being used; this should be
  used to get the :class:`Collection` object from :attr:`db` in Javascript
  code
* ``query`` -- the query that has been generated by the
  :class:`~mongomotor.queryset.QuerySet` object; this may be passed into
  the :meth:`find` method on a :class:`Collection` object in the Javascript
  function
* ``options`` -- an object containing the keyword arguments passed into
  :meth:`~mongomotor.queryset.QuerySet.exec_js`

The following example demonstrates the intended usage of
:meth:`~mongomotor.queryset.QuerySet.exec_js` by defining a function that sums
over a field on a document (this functionality is already available through
:meth:`~mongomotor.queryset.QuerySet.sum` but is shown here for sake of
example)::

    def sum_field(document, field_name, include_negatives=True):
        code = """
        function(sumField) {
            var total = 0.0;
            db[collection].find(query).forEach(function(doc) {
                var val = doc[sumField];
                if (val >= 0.0 || options.includeNegatives) {
                    total += val;
                }
            });
            return total;
        }
        """
        options = {'includeNegatives': include_negatives}
        await document.objects.exec_js(code, field_name, **options)


As fields in MongoMotor may use different names in the database (set using the
:attr:`db_field` keyword argument to a :class:`Field` constructor), a mechanism
exists for replacing MongoEngine field names with the database field names in
Javascript code. When accessing a field on a collection object, use
square-bracket notation, and prefix the MongoEngine field name with a tilde.
The field name that follows the tilde will be translated to the name used in
the database. Note that when referring to fields on embedded documents,
the name of the :class:`~mongomotor.fields.EmbeddedDocumentField`, followed by
a dot, should be used before the name of the field on the embedded document.
The following example shows how the substitutions are made::

    class Comment(EmbeddedDocument):
        content = StringField(db_field='body')

    class BlogPost(Document):
        title = StringField(db_field='doctitle')
        comments = ListField(EmbeddedDocumentField(Comment), name='cs')

    # Returns a list of dictionaries. Each dictionary contains a value named
    # "document", which corresponds to the "title" field on a BlogPost, and
    # "comment", which corresponds to an individual comment. The substitutions
    # made are shown in the comments.
    BlogPost.objects.exec_js("""
    function() {
        var comments = [];
        db[collection].find(query).forEach(function(doc) {
            // doc[~comments] -> doc["cs"]
            var docComments = doc[~comments];

            for (var i = 0; i < docComments.length; i++) {
                // doc[~comments][i] -> doc["cs"][i]
                var comment = doc[~comments][i];

                comments.push({
                    // doc[~title] -> doc["doctitle"]
                    'document': doc[~title],

                    // comment[~comments.content] -> comment["body"]
                    'comment': comment[~comments.content]
                });
            }
        });
        return comments;
    }
    """)
