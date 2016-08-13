:tocdepth: 1

MongoMotor: An asynchronous object-document mapper for Python and MongoDB
=========================================================================

|mongomotor-logo|

.. |mongomotor-logo| image:: ./_static/mongomotor.jpg
    :alt: Asynchronous object-document mapper for Python and MongoDB


MongoMotor is a simple-to-use, declarative-style, document-object mapper
that puts together the nice `MongoEngine <http://mongoengine.org/>`_ API
and the clever `Motor <http://motor.readthedocs.org/en/stable/>`_ asynchronous
approuch to create a awesome for solution for asynchronous access to MongoDB
using python 3.4+.

Installation
============

To install it using pip:

.. code-block:: sh

    $ pip install mongomotor


And that's it!


MongoMotor usage
================

MongoMotor can be used with `Tornado <http://tornadoweb.org/>`_ or with
`asyncio <https://docs.python.org/3/library/asyncio.html>`_. In its
introduction tutorial we will create a simple music catalog. Proceed to the
tutorial that interests you.


.. toctree::
   :maxdepth: 1

   tutorial_asyncio



API Documentation
=================

.. toctree::
   :maxdepth: 1

   apidoc/modules


Licence
=======

MongoMotor is free software, licensed under the GPL version 3 or latter.


Contributing
============

MongoMotor's code is hosted on
`gitlab <https://gitlab.com/mongomotor/mongomotor>`_ and there is the
`issue tracker <https://gitlab.com/mongomotor/mongomotor/issues>`_, too.
Feel free to create a fork of the project, open issues, do merge requests...


Changelog
=========

v0.8.2
++++++

* Correcting __get__ on empty ComplexBaseField.

v0.8.1
++++++

* Correcting __getitem__ on queryset

v0.8
++++

* Backing ComplexBaseField and ReferenceField behavior to old one. It
  now always returns a future. The other way was confusing.

v0.7
++++++

* Added eager_on on get() method of queryset.
* Corrected how ComplexBaseField and ReferenceField handle references.
  Now it only returns a future when the database is really reached.
* Adding modify() to queryset
* Updating motor to 0.6.2. That changed aggregate interface. It now returns
  a cursor.


Documentation translations
==========================

`Documentação do MongoMotor em português <http://mongomotor.poraodojuca.net/ptbr/>`_


Well, that's it!
Thank you!
