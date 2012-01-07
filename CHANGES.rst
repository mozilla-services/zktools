Changelog
=========

0.2 (**tip**)
-------------

Changes
*******

- Added context manager return to lock to allow use of the 'with'
  statement.

Features
********

- Configuration object to load/store configuration data in Zookeeper.

Backward Incompatibilities
**************************

- SharedZkLock has been refactored into ZkWriteLock and ZkReadLock.
- ``reconnect`` is no longer an option to ZkConnection.


0.1 (11/22/2011)
----------------

Features
********

- Lock implementation, with revokable shared locks.
- Zookeeper connection object with automatic reconnect.
