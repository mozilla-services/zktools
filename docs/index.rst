===============
Zookeeper Tools
===============

A Python collection of higher-level API's built on top of `Apache Zookeeper`_,
a distributed, highly reliable coordination service with no single point of
failure.

Features:

* :mod:`Locks <zktools.locking>` - Support for non-blocking acquire, shared
  read/write locks, and modeled on Python's
  Lock objects that also includes `Revocable Shared Locks with Freaking 
  Laser Beams` as described in the `Zookeeper Recipes`_.
* :mod:`Nodes <zktools.node>` - Objects to track values in Zookeeper
  Nodes (zNodes) automatically.

Reference Material
==================

Reference material includes documentation for every `zktools` API.

.. toctree::
   :maxdepth: 1

   api
   Changelog <changelog>

Source Code
===========

All source code is available on `github under zktools <https://github.com/mozilla-services/zktools>`_.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`glossary`

License
=======

``zktools`` is offered under the MPL license.

Authors
=======

``zktools`` is made available by the `Mozilla Foundation`.

.. _Apache Zookeeper: http://zookeeper.apache.org/
.. _Zookeeper Recipes: http://zookeeper.apache.org/doc/current/recipes.html#sc_recoverableSharedLocks
