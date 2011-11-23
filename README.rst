===============
Zookeeper Tools
===============

``zktools`` is a package of tools implementating higher level constructs using
`Apache Zookeeper`_.

It currently provides:

* A Zookeeper Connection object that handles proxying calls to zookeeper
  using the correct zookeeper handle, and re-establishing the connection
  during connection loss in a call.
* A Zookeeper lock with support for non-blocking acquire, modeled on Python's
  Lock objects that also includes a `Revocable Shared Locks with Freaking Laser
  Beams` described in the `Zookeeper Recipe's <http://zookeeper.apache.org/doc/current/recipes.html#sc_recoverableSharedLocks>`_.

See `the full docs`_ for more  information.

License
=======

``zktools`` is offered under the MPL license.

Authors
=======

``zktools`` is made available by the `Mozilla Foundation`.

.. _Apache Zookeeper: http://zookeeper.apache.org/
.. _the full docs: http://zktools.rtfd.org/
