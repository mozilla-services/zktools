===============
Zookeeper Tools
===============

A collection of higher-level API's built on top of `Apache Zookeeper`_, a
distributed, highly reliable coordination service with no single point of
failure.

Features:

* ``Connection Object`` -  A :class:`Zookeeper Connection 
  <zktools.connection.ZkConnection>` object that handles proxying calls to
  zookeeper using the correct zookeeper handle, and re-establishing the
  connection during connection loss in a call.
* ``Configuration`` - :mod:`Zookeeper Configuration Helpers
  <zktools.configuration>` to store and load configuration information stored
  in Zookeeper nodes.
* ``Locks`` - A :mod:`Zookeeper lock <zktools.locking>` with support for
  non-blocking acquire, modeled on Python's Lock objects that also includes a
  `Revocable Shared Locks with Freaking Laser Beams` described in the 
  `Zookeeper Recipe's 
  <http://zookeeper.apache.org/doc/current/recipes.html#sc_recoverableSharedLocks>`_.

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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`

Module Listing
--------------

.. toctree::
    :maxdepth: 2
    
    api/index


.. include:: ../CHANGES.rst

.. _Apache Zookeeper: http://zookeeper.apache.org/
