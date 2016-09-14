======
GridFS
======

Writing
-------

GridFS support comes in the form of the :class:`~mongomotor.fields.FileField`
field object. This field acts as a file-like object and provides a couple of
different ways of inserting and retrieving data. Arbitrary metadata such as
content type can also be stored alongside the files. In the following example,
a document is created to store details about animals, including a photo:

.. code-block:: python

    class Animal(Document):
        genus = StringField()
        family = StringField()
        photo = FileField()

    marmot = Animal(genus='Marmota', family='Sciuridae')

    marmot_photo = open('marmot.jpg', 'rb')
    yield from marmot.photo.put(marmot_photo, content_type = 'image/jpeg')
    yield from marmot.save()

Retrieval
---------

So using the :class:`~mongomotor.fields.FileField` is just like using any other
field. The file can also be retrieved just as easily::

    marmot = Animal.objects(genus='Marmota').first()
    photo = marmot.photo.read()
    content_type = marmot.photo.content_type

Streaming
---------

Streaming data into a :class:`~mongomotor.fields.FileField` is achieved in a
slightly different manner.  First, a new file must be created by calling the
:func:`new_file` method. Data can then be written using :func:`write`:

.. code-block:: python

    yield from marmot.photo.new_file()
    yield from marmot.photo.write('some_image_data')
    yield from marmot.photo.write('some_more_image_data')
    yield from marmot.photo.close()

    yield from marmot.save()

Deletion
--------

Deleting stored files is achieved with the :func:`delete` method:

.. code-block:: python

    yield from marmot.photo.delete()

.. warning::

    The FileField in a Document actually only stores the ID of a file in a
    separate GridFS collection. This means that deleting a document
    with a defined FileField does not actually delete the file. You must be
    careful to delete any files in a Document as above before deleting the
    Document itself.


Replacing files
---------------

Files can be replaced with the :func:`replace` method. This works just like
the :func:`put` method so even metadata can (and should) be replaced:

.. code-block:: python

    another_marmot = open('another_marmot.png', 'rb')
    yield from marmot.photo.replace(another_marmot, content_type='image/png')
