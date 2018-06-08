Changelog
=========

v0.13.0
+++++++

* Dropped support for python 3.4 and tornado syntax because torando supports
  await/async syntax. That makes the code much easier to maintain.

v0.12.0
+++++++

* Added :class:`~mongomotor.queryset.QuerySetNoCache`

v0.11.3
+++++++

* Corrected multiple cascade delete rules

v0.11.2
+++++++

* corrected cascade delete rule without reference

v0.11.1
+++++++

* Corrected asynchronous on non-main threads

v0.11.0
+++++++

* Changing signals to AsyncBlink signals. Now coroutines can be used as
  receivers for signals

* Tornado is now an optional dependency.

v0.10.1
+++++++

* Corrected Document update() method.


v0.10.0
+++++++

* Added asynchronous GenericReferenceField

v0.9.4
++++++

* Motor updated. Now asynchronize stuff is done with a thread pool instad
  of greenlets

v0.9.3
++++++

* Corrected authentication.

v0.9.2
++++++

* Corrected dereferencing of objects in lists.

v0.9.1
++++++

* Adding DynamicEmbeddedDocument to the api

v0.9
++++

* Completly re-wrote. Now it supports asyncio and tornado, just as motor
   does.
* Now it supports GridFS

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
