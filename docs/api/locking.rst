.. _locking_module:

:mod:`zktools.locking`
======================

.. automodule:: zktools.locking


Constants
---------

.. data:: IMMEDIATE
    
    Flag used to declare that revokation should occur immediately. Other
    lock-holders will not be given time to release their lock.

Lock Class
----------

.. autoclass:: ZkLock
    :members: __init__, acquire, release, revoked, revoke_all, has_lock, clear

Shared Read/Write Lock Classes
------------------------------

.. autoclass:: ZkReadLock
	:members: __init__, acquire, revoked, has_lock, revoke_all, release, clear

.. autoclass:: ZkWriteLock
    :members: __init__, acquire, revoked, has_lock, revoke_all, release, clear

Private Lock Base Class
-----------------------

.. autoclass:: _LockBase
    :members: __init__, _acquire_lock, release, revoked, has_lock, clear

Internal Utility Functions
--------------------------

.. autofunction:: has_read_lock
.. autofunction:: has_write_lock
