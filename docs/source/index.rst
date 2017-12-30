:tocdepth: 1

MongoMotor: Asynchronous Object-Document Mapper
===============================================

|mongomotor-logo|

.. |mongomotor-logo| image:: ./_static/mongomotor.jpg
    :alt: Asynchronous object-document mapper for Python and MongoDB


MongoMotor is a simple-to-use, declarative-style, document-object mapper
that puts together the nice `MongoEngine <http://mongoengine.org/>`_ API
and the clever `Motor <http://motor.readthedocs.org/en/stable/>`_ asynchronous
approuch to create a awesome library for asynchronous access to MongoDB
using python 3.4+.

Installation
============

To install MongoMotor, use pip:

.. code-block:: sh

    $ pip install mongomotor

.. note::

   If you want to use mongomotor with tornado, you can install tornado as a
   dependency with:

   .. code-block:: sh

      $ pip install mongomotor[tornado]


And that's it!


Usage
=====

MongoMotor can be used with `Tornado <http://tornadoweb.org/>`_ or with
`asyncio <https://docs.python.org/3/library/asyncio.html>`_. In its
introduction tutorial we will create a simple music catalog. Proceed to the
tutorial that interests you.


.. toctree::
   :maxdepth: 1

   tutorial_asyncio
   tutorial_tornado


Guides
======

.. toctree::
   :maxdepth: 1

   guide/defining-documents
   guide/querying
   guide/gridfs


API Documentation
=================

.. toctree::
   :maxdepth: 1

   apidoc/modules


CHANGELOG
=========

.. toctree::
   :maxdepth: 1

   CHANGELOG

Licence
=======

MongoMotor is free software, licensed under the GPL version 3 or latter.


Contributing
============

MongoMotor's code is hosted on
`github <https://github.com/jucacrispim/mongomotor>`_. Feel free to create
a fork of the project, open issues, do merge requests...


Well, that's it!
Thank you!
