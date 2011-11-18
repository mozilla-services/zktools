:mod:`zktools.locking` -- Zookeeper Locking Classes
===================================================

.. automodule:: zktools.locking

Implements locking functionality.

Module Contents
---------------

.. autoclass:: ZkLock
    :members: __init__, acquire, renew, release, has_lock, clear

.. autoclass: SharedZkLock
	:members: acquire_read_lock, acquire_write_lock
