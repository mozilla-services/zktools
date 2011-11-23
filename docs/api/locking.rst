:mod:`zktools.locking` -- Zookeeper Locking Classes
===================================================

.. automodule:: zktools.locking


Module Contents
---------------

.. data:: IMMEDIATE
    
    Flag used to declare that revokation should occur immediately. Other
    lock-holders will not be given time to release their lock.

.. autoclass:: _LockBase
    :members: __init__, _acquire_lock, release, revoked, has_lock, clear

.. autoclass:: ZkLock
    :members: __init__, acquire, release, revoked, has_lock, clear

.. autoclass:: SharedZkLock
	:members: __init__, acquire_read_lock, acquire_write_lock, revoked, has_lock, release, clear

Internal Utility Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: has_read_lock
.. autofunction:: has_write_lock
