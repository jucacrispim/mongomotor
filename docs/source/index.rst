.. mongomotor documentation master file, created by
   sphinx-quickstart on Thu Sep  4 15:39:23 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to mongomotor's documentation!
======================================

Mongomotor is an integration between mongoengine and motor. Using mongomotor
you can write your models in the same way you write with mongoengine and fetch
your results in an async way through the use of motor.


Install
=======

First, clone the project on gitorious.

.. code-block:: sh

    $ git clone https://gitorious.org/mongomotor/mongomotor.git

Then, install the dependencies:

.. code-block:: sh

    $ cd mongomotor && pip install -r requirements.txt

Finally, run the tests

.. code-block:: sh

    $ python setup.py test


.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
