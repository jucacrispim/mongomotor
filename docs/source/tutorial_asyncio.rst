Using MongoMotor with asyncio
=============================

In this introductory tutorial lets create a simple music catalog. In this
catalog we'll have artists, albums and musics.


Connecting to a database
++++++++++++++++++++++++

The first thing to do is to connect to a database (an instance of mongod).
To do so we will use :func:`~mongomotor.connect`. In this example all we
need is to call the function with a single parameter, the database name.


.. code-block:: python

    from mongomotor import connect

    connect('music-catalog')


.. note::

    :func:`~mongomotor.connect` accepts a ``async_framework`` named parameter
    that indicates which asynchronous framework we should use, either
    ``asyncio`` or ``tornado``. It defaults to ``asyncio``. The other arguments
    to :func:`~mongomotor.connect` are passed to mongoengine's connect()
    function. See the
    `Connecting guide <http://docs.mongoengine.org/guide/connecting.html>`_
    at mongoengine docs.


Defining documents
++++++++++++++++++

MongoDB does not enforce a schema in the database making life a lot easier
when we want to add or remove fields. However is very useful to have a schemata
defined to help us in the development process this is why we'll define some
documents here.


Artists
-------

In our example we will create a collection of documents to store information
about artists. To create a document that have its own collection we need to
subclass :class:`~mongomotor.Document`. Let's say that we want to distinguish
between two kinds of artists, single musicians, those that release solo
albums and musical groups. To achieve that we can have a base *Artist* and
a *SingleMusician* and a *MusicalGroup* inheriting from *Artist*. Doing so
all artists will be stored in the same collection and we can query for
them separately (or together if we want to to so).


.. code-block:: python

    from mongomotor import Document
    from mongomotor.fields import ListField, StringField


    class Artist(Document):
	name = StringField()

	# here we say that this call may be subclassed.
	# If we do not set it to True we will not be able
	# to subclass Artist.
	meta = {'allow_inheritance': True}


    class SingleMusician(Artist):
	real_name = StringField()


    class MusicalGroup(Artist):
	people = ListField(StringField())


With the documents created here we have a document *Artist* with a ``name``
field that accepts strings.The *SingleMusician* document has, other than
``name``, the ``real_name`` that accepts a string too. The *MusicalGroup*
document has a ``people`` field that is a list of strings (the names of
the members of the group).


Musics & Albums
---------------

Now we will create a collection of documents for albums and
the musics will be embedded documents in the album document. Albums will
have references for artists. For more about embedded documents and references
take a look at the MongoDB
`docs <https://docs.mongodb.com/manual/core/data-modeling-introduction/>`_.


.. code-block:: python

    from mongomotor import EmbeddedDocument
    from mongomotor.fields import ReferenceField, EmbeddedDocumentField, IntField()


    class Music(EmbeddedDocument):
        number = IntField()
	title = StringField()


    class Album(Document):
	title = StringField()
	musics = ListField(EmbeddedDocumentField(Music))
	artists = ListField(ReferenceField(Artist))


In these documents we have some different things. First we have the document
*Music* that is an embedded document in the document collection. To reference
to an embedded document we need to use
:class:`~mongomotor.fields.EmbeddedDocumentField`. Other than that we have
a :class:`~mongomotor.fields.ReferenceField` referencing *Artist*. In both
cases we are using a list of embedded documents or references, but if we
wanted, for example, only one artist per album we could use:

.. code-block:: python

    class Album(Document):
	title = StringField()
	tracks = ListField(EmbeddedDocumentField(Music))
	# this is only to show how that could works. Let's keep with
	# our list of artists in the rest of the example.
	artist = ReferenceField(Artist)

For more see :doc:`guide/defining-documents`.


Insert and retrieving data
++++++++++++++++++++++++++

With our documents' schema defined let's add some documents to our database.


Inserting data
--------------

First let's create some artists by creating an instance of *SingleMusician*
or *MusicalGroup* and then use the :meth:`~mongomotor.document.Document.save`
in a ``yield from`` statement.

.. note::

   All mongomotor database operations are done in coroutines and need a
   event loop running to succed. In these examples we will run only the
   database methods inside a coroutine and consume this coroutine
   with run_until_complete. In real life usually things are
   different we usually call run_until_complete only once.
   For more information see:
   `asyncio loop <https://docs.python.org/3/library/asyncio-eventloop.html>`_.


.. code-block:: python

   >>> import asyncio
   >>> loop = asyncio.get_event_loop()
   >>> artist = SingleMusician(name='Tim Maia', real_name='Sebastião Maia')
   >>> group = MusicalGroup()
   >>> group.name = 'j.m.k.e.'
   >>> group.people = ['Villu', 'Reimo', 'Andres', 'Livia', 'Promille']
   >>>
   >>> async def insert_artist():
   ...     await artist.save()
   ...     await group.save()
   ...     print(artist.id)
   ...     print(group.id)
   ...
   >>> loop.run_until_complete(insert_artist())
   57ac52e27c1c8440398a347e
   57ac56767c1c8440398a347f

If you are using Python 3.4 you must to use the ``asyncio.coroutine`` decorator
and ``yield from`` instead of ``await``.

.. code-block:: python

   >>> import asyncio
   >>> loop = asyncio.get_event_loop()
   >>> artist = SingleMusician(name='Tim Maia', real_name='Sebastião Maia')
   >>> group = MusicalGroup()
   >>> group.name = 'j.m.k.e.'
   >>> group.people = ['Villu', 'Reimo', 'Andres', 'Livia', 'Promille']
   >>>
   >>> @asycio.coroutine
   ... def insert_artist():
   ...     yield from artist.save()
   ...     yield from group.save()
   ...     print(artist.id)
   ...     print(group.id)
   ...
   >>> loop.run_until_complete(insert_artist())
   57ac52e27c1c8440398a347e
   57ac56767c1c8440398a347f


As you can see, an ID was created automatically when the document was saved to
the database. Now, let's create some albums and reference the artists in
the albums.

.. code-block:: python

   >>> album1 = Album(title="Racional Vol. 1", artists=[artist])
   >>> titles = ['Imunização Racional (Que beleza)', 'O Grão Mestre Varonil']
   >>> album1.tracks = [Music(title=t, number=i) for i, t in enumerate(titles)]
   >>> album2 = Album(title='Mälestusi Eesti NSV-st')
   >>> titles = ['Medal', 'Ma ei saa sust aru']
   >>> album2.tracks = [Music(title=t, number=i) for i, t in enumerate(titles)]
   >>> # Now we will save the documents to the db. We don't use save() for
   >>> # embedded documents.
   >>> async def insert_albums():
   ...     await album1.save()
   ...     await from album2.save()
   ...
   >>> loop.run_until_complete(insert_albums())


Retrieving data
---------------

Now we have some data and it is time to retrieve it from database. This is done
throught the attribuite ``objects``, that is a instance of
:class:`~mongomotor.queryset.QuerySet`, in the subclasses of
:class:`~mongomotor.Document`.

The simplest way of retrieving data is quering for a specific document using
:meth:`~mongomotor.queryset.QuerySet.get`.

.. code-block:: python

   >>> async def get_artist():
   ...     artist = await Artist.objects.get(name='Tim Maia')
   ...     print(artist.id, artist.real_name)
   ...
   >>> loop.run_until_complete(get_artist())

.. note::

   If a query does not return any documents or returns more than one document,
   the method ``get()`` will raise an exception.

We can query for more than one document we may use
:meth:`~mongomotor.queryset.QuerySet.filter`. This method returns a queryset.
To iterate over a queryset we use ``async for``.

.. code-block:: python

   >>> async def list_artists():
   ...     async for artist in Artist.objects:
   ...         print(artist.name)
   ...         async for album in Album.objects.filter(artists=artist):
   ...             print(' - {}'.format(album.title))
   ...             for track in album.tracks:
   ...                 print('  - {}'.format(track.title))
   ...
   >>> loop.run_until_complete(list_artists())


In Python 3.4 you must use a ``while`` loop and call
:meth:`~mongomotor.queryset.QuerySet.fetch_next` in a ``yield from``
statement and then use :meth:`~mongomotor.queryset.QuerySet.next_object`.

.. code-block:: python

   >>> @asyncio.coroutine
   ... def list_artists():
   ...     artists = Artist.objects:
   ...     while (yield from artists.fetch_next):
   ...         artist = artists.next_object()
   ...         albums = Album.objects.filter(artists=artist)
   ...         print(artist.name)
   ...         while (yield from albums.fetch_next):
   ...             album = albums.next_object()
   ...             print(' - {}'.format(album.title))
   ...             for track in album.tracks:
   ...                 print('  - {}'.format(track.title))
   ...
   >>> loop.run_until_complete(list_artists())



For more information see :doc:`guide/querying`.
