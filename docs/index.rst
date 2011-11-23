===============
Zookeeper Tools
===============

``zktools`` is a package of tools implementating higher level constructs using
`Apache Zookeeper`_.   

It currently provides:

* A :class:`Zookeeper Connection <zktools.connection.ZkConnection>` object that
  handles proxying calls to zookeeper using the correct zookeeper handle, and 
  re-establishing the connection during connection loss in a call.
* A :mod:`Zookeeper lock <zktools.locking>` with support for
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
