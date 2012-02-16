Changelog
=========

0.2.1 (02/16/2012)
------------------

- Fixed packaging bug.


0.2 (02/03/2012)
----------------

Changes
*******

- Added context manager return to lock to allow use of the 'with'
  statement.
- Refactored to use zc.zk ZooKeeper library for higher level Zookeeper
  abstraction with automatic watch re-establishment.

Features
********

- Node object to retrieve ZNode data from Zookeeper and keep it up
  to date.
- Node objects can have data and children subscribers.
- NodeDict object that maps a shallow tree (one level of children)
  into a dict-like object.

Backward Incompatibilities
**************************

- SharedZkLock has been refactored into ZkWriteLock and ZkReadLock.
- ``revoked`` is a property of Locks, not a method.
- ZkConnection is gone, lock objects, ZkNode, and ZkNodeDict all expect
  zc.zk ZooKeeper instances.


0.1 (11/22/2011)
----------------

Features
********

- Lock implementation, with revokable shared locks.
- Zookeeper connection object with automatic reconnect.
