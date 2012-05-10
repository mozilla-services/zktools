Changelog
=========

0.3 (unreleased)
----------------

Changes
*******

- Use the Zookeeper-provided mtime of a node as the last_modified
  attribute, instead of client specific time.time()
- Use GUID from new Zookeeper recipe to handle connection loss which
  still results in a created node.
- Add safe-call to ensure that commands run reliably in the face of a
  connection loss exception during their call, and have lock code run
  in a separate thread to ensure it doesn't deadlock the ZK event
  thread.
- Removed zc-zookeeper-static requirement as some OS distributions include
  the python zookeeper binding as a system package.

Bugfixes
********

- Fix ZkNode issues with dead-locks due to having locking code inside
  watch callbacks.

Backward Incompatibilities
**************************

- Removed ZkNodeDict which establishes a questionable amount of watches on
  the server. Setting too many watches has multiple drawbacks, larger values
  should be stored as JSON in a single node.

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
