===============
Zookeeper Tools
===============

A collection of higher-level API's built on top of `Apache Zookeeper`_, a
distributed, highly reliable coordination service with no single point of
failure.

Features:

* **Connection Object** -  A :class:`Zookeeper Connection 
  <zktools.connection.ZkConnection>` object that handles proxying calls to
  zookeeper using the correct zookeeper handle, and re-establishing the
  connection during connection loss in a call.
* **Locks** - A :mod:`Zookeeper lock <zktools.locking>` with support for
  non-blocking acquire, shared read/write locks, and modeled on Python's
  Lock objects that also includes `Revocable Shared Locks with Freaking 
  Laser Beams` described in the `Zookeeper Recipes`_.
* **Nodes** - :mod:`Zookeeper Node <zktools.node>` objects to track values in Zookeeper
  Nodes automatically.

Reference Material
==================

Reference material includes documentation for every `zktools` API.

.. toctree::
   :maxdepth: 1

   api
   Changelog <changelog>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`

License
=======

``zktools`` is offered under the MPL license.

Authors
=======

``zktools`` is made available by the `Mozilla Foundation`.

Source
======

Source code can be found on `the github zktools repository 
<https://github.com/mozilla-services/zktools>`_.

.. _Apache Zookeeper: http://zookeeper.apache.org/
.. _Zookeeper Recipes: http://zookeeper.apache.org/doc/current/recipes.html#sc_recoverableSharedLocks
