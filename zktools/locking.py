# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Zookeeper Locking

This module provides a :class:`ZkLock`, which should look familiar to anyone
that has used Python's ``threading.Lock`` class. In addition to normal locking
behavior, revokable shared read/write locks are also supported. All of the
locks can be revoked as desired. This requires the current lock holder(s) to
release their lock(s).

**Shared Read/Write Locks**

Also known in the Zookeeper Recipes as ``Revocable Shared Locks with Freaking
Laser Beams``, :class:`ZkReadLock` and :class:`ZkWriteLock` locks have been
implemented. A read lock can be acquired as long as no write locks are active,
while a write-lock can only be acquired when there are no other read or write
locks active.

**Using the Lock Command Line Interface**

`zktools` comes with a CLI to easily see current locks, details of each
lock, and remove empty locks called `zooky`.

Usage:

.. code-block:: bash

    $ zooky list
    LOCK                           STATUS
    fred                           Locked
    zkLockTest                     Free

    $ zooky show fred
    LOCK HOLDER          DATA            INFO
    write-0000000002     0               {'pzxid': 152321L, 'ctime': 1326417195365L, 'aversion': 0, 'mzxid': 152321L, 'numChildren': 0,
                                         'ephemeralOwner': 86927055090548768L, 'version': 0, 'dataLength': 1, 'mtime': 1326417195365L,
                                         'cversion': 0, 'modifed_ago': 16, 'created_ago': 16, 'czxid': 152321L}

The `modifed_ago` and `created_ago` fields in INFO show how many seconds
ago the lock was created and modified.

"""
import logging
import threading
import time
from optparse import OptionParser

from clint.textui import colored
from clint.textui import columns
from clint.textui import puts
import zookeeper

from zc.zk import ZooKeeper

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}
IMMEDIATE = object()

log = logging.getLogger(__name__)


class _LockBase(object):
    """Base lock implementation for subclasses"""
    def __init__(self, connection, lock_name, lock_root='/ZktoolsLocks',
                 logfile=None):
        """Create a Zookeeper lock object

        :param connection: Zookeeper connection object
        :type connection: zc.zk Zookeeper instance
        :param lock_root: Path to the root lock node to create the locks
                          under
        :type lock_root: string
        :param logfile: Path to a file to log the Zookeeper stream to
        :type logfile: string

        """
        self._zk = connection
        self._cv = threading.Condition()
        self._lock_root = lock_root
        self._locks = threading.local()
        self._locks.revoked = []
        self._locks.lock_args = ([], {})
        self._log_debug = logging.DEBUG >= log.getEffectiveLevel()
        if logfile:
            zookeeper.set_log_stream(open(logfile))
        else:
            zookeeper.set_log_stream(open("/dev/null"))
        self._locknode = '%s/%s' % (self._lock_root, lock_name)
        self._ensure_lock_dir()

    def _ensure_lock_dir(self):
        # Ensure our lock dir exists
        if self._zk.exists(self._locknode):
            return

        try:
            self._zk.create(self._lock_root, "zktools ZLock dir",
                           [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            if self._log_debug:
                log.debug("Lock node in Zookeeper already created")

        # Try and create our locking node
        try:
            self._zk.create(self._locknode, "lock", [ZOO_OPEN_ACL_UNSAFE],
                            0)
        except zookeeper.NodeExistsException:
            # Ok if this exists already
            pass

    def _acquire_lock(self, node_name, timeout=None, revoke=False):
        """Acquire a lock

        Internal function used by read/write lock

        :param node_name: Name of the node to use for the lock
        :type node_name: str
        :param timeout: How long to wait to acquire the lock, set to 0 to
                        get non-blocking behavior.
        :type timeout: int
        :param revoke: Whether prior locks should be revoked. Can be set to
                       True to request and wait for prior locks to release
                       their lock, or :obj:`IMMEDIATE` to destroy the blocking
                       read/write locks and attempt to acquire a write lock.
        :type revoke: bool or :obj:``IMMEDIATE``


        :returns: True if the lock was acquired, False otherwise
        :rtype: bool

        """
        # First clear out any prior revocation warnings
        self._locks.revoked = []
        revoke_lock = self._locks.revoked

        # Create a lock node
        znode = self._zk.create(self._locknode + node_name, "0",
                               [ZOO_OPEN_ACL_UNSAFE],
                               zookeeper.EPHEMERAL | zookeeper.SEQUENCE)

        def revoke_watcher(handle, type, state, path):
            # This method must be in closure scope to ensure that
            # it can append to the thread it is called from
            # to indicate if this particular thread's lock was
            # revoked or removed
            if type == zookeeper.CHANGED_EVENT:
                data = self._zk.get(path, revoke_watcher)[0]
                if data == 'unlock':
                    revoke_lock.append(True)
            elif type == zookeeper.DELETED_EVENT or \
                 state == zookeeper.EXPIRED_SESSION_STATE:
                # Trigger if node was deleted
                revoke_lock.append(True)

        data = self._zk.get(znode, revoke_watcher)[0]
        if data == 'unlock':
            revoke_lock.append(True)
        keyname = znode[znode.rfind('/') + 1:]

        acquired = False
        cv = threading.Event()

        def lock_watcher(handle, type, state, path):
            cv.set()

        lock_start = time.time()
        first_run = True
        while not acquired:
            cv.clear()

            # Have we been at this longer than the timeout?
            if not first_run:
                if timeout is not None and time.time() - lock_start > timeout:
                    try:
                        self._zk.delete(znode)
                    except zookeeper.NoNodeException:
                        pass
                    return False

            # Get all the children of the node
            children = self._zk.get_children(self._locknode)
            children.sort(key=lambda val: val[val.rfind('-') + 1:])

            if len(children) == 0 or not keyname in children:
                # Disconnects or other errors can cause this
                znode = self._zk.create(
                    self._locknode + node_name, "0", [ZOO_OPEN_ACL_UNSAFE],
                    zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
                keyname = znode[znode.rfind('/') + 1:]
                data = self._zk.get(znode, revoke_watcher)[0]
                if data == 'unlock':
                    revoke_lock.append(True)
                continue
            acquired, blocking_nodes = self._locks.has_lock(keyname, children)
            if acquired:
                break

            if revoke == IMMEDIATE:
                # Remove all prior nodes
                for node in blocking_nodes:
                    try:
                        self._zk.delete(self._locknode + '/' + node)
                    except zookeeper.NoNodeException:
                        pass
                continue  # Now try again
            elif revoke:
                # Ask all prior blocking nodes to release
                for node in blocking_nodes:
                    try:
                        self._zk.set(self._locknode + '/' + node, "unlock")
                    except zookeeper.NoNodeException:
                        pass
            prior_blocking_node = self._locknode + '/' + blocking_nodes[-1]
            exists = self._zk.exists(prior_blocking_node, lock_watcher)
            if not exists:
                # The node disappeared? Rinse and repeat.
                continue

            # Wait for a notification from get_children, no longer
            # than the timeout
            wait_for = None
            if timeout is not None:
                time_spent = time.time() - lock_start
                wait_for = timeout - time_spent
            cv.wait(wait_for)
            first_run = False
        self._locks.lock_node = znode
        return True

    def __call__(self, *args, **kwargs):
        self._locks.lock_args = (args, kwargs)
        return self

    def __enter__(self):
        args, kwargs = getattr(self._locks, 'lock_args', ([], {}))
        self.acquire(*args, **kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        self._locks.lock_args = ([], {})
        self.release()

    def release(self):
        """Release a lock

        :returns: True if the lock was released, or False if it is no
                  longer valid.
        :rtype: bool

        """
        self._locks.revoked = []
        try:
            self._zk.delete(self._locks.lock_node)
            del self._locks.lock_node
            return True
        except (zookeeper.NoNodeException, AttributeError):
            return False

    def has_lock(self):
        """Check with Zookeeper to see if the lock is acquired

        :returns: Whether the lock is acquired or not
        :rtype: bool

        """
        if not hasattr(self._locks, 'lock_node'):
            # So we can check it even if we released
            return False

        znode = self._locks.lock_node
        keyname = znode[znode.rfind('/') + 1:]
        # Get all the children of the node
        children = self._zk.get_children(self._locknode)
        children.sort(key=lambda val: val[val.rfind('-') + 1:])
        if keyname not in children:
            return False

        acquired = self._locks.has_lock(keyname, children)[0]
        return bool(acquired)

    def clear(self):
        """Clear out a lock

        .. warning::

            You must be sure this is a dead lock, as clearing it will
            forcibly release it by deleting all lock nodes.

        :returns: True if the lock was cleared, or False if it
                  is no longer valid.
        :rtype: bool

        """
        children = self._zk.get_children(self._locknode)
        for child in children:
            try:
                self._zk.delete(self._locknode + '/' + child)
            except zookeeper.NoNodeException:
                pass

    def revoke_all(self):
        """Revoke any existing locks, gently

        Unlike :meth:`clear`, this asks all existing locks to
        release, rather than forcibly revoking them.

        :returns: True if existing locks were present, False if
                  there were no existing locks.
        :rtype: bool

        """
        # Get all the children of the node
        children = self._zk.get_children(self._locknode)
        if not children:
            return False

        for child in children:
            try:
                self._zk.set(self._locknode + '/' + child, "unlock")
            except zookeeper.NoNodeException:
                pass
        return True

    @property
    def revoked(self):
        """Indicate if this shared lock has been revoked

        :returns: True if the lock has been revoked, False otherwise.
        :rtype: bool
        """
        return bool(self._locks.revoked)

    @property
    def connected(self):
        """Indicate whether a connection to Zookeeper exists"""
        return self._zk.connected


class ZkLock(_LockBase):
    """Zookeeper Lock

    Implements a Zookeeper based lock optionally with lock revocation
    should locks be idle for more than a specific set of time.

    Example::

        from zc.zk import ZooKeeper
        from zktools.locking import ZkLock

        # Create a connection and a lock
        conn = ZooKeeper()
        my_lock = ZkLock(conn, "my_lock_name")

        my_lock.acquire() # wait to acquire lock
        # do something with the lock

        my_lock.release() # release our lock

        # Or, using the context manager
        with my_lock:
            # do something with the lock

    """
    def acquire(self, timeout=None, revoke=False):
        """Acquire a lock

        :param timeout: How long to wait to acquire the lock, set to 0 to
                        get non-blocking behavior.
        :type timeout: int
        :param revoke: Whether prior locks should be revoked. Can be set to
                       True to request and wait for prior locks to release
                       their lock, or :obj:`IMMEDIATE` to destroy the blocking
                       read/write locks and attempt to acquire a write lock.
        :type revoke: bool or :obj:`IMMEDIATE`

        :returns: True if the lock was acquired, False otherwise
        :rtype: bool

        """
        node_name = '/lock-'
        self._locks.has_lock = has_write_lock
        return self._acquire_lock(node_name, timeout, revoke)


class ZkReadLock(_LockBase):
    """Shared Zookeeper Read Lock

    A read-lock is considered successful if there are no active write
    locks.

    This class takes the same initialization parameters as
    :class:`ZkLock`.

    """
    def acquire(self, timeout=None, revoke=False):
        """Acquire a shared read lock

        :param timeout: How long to wait to acquire the lock, set to 0 to
                        get non-blocking behavior.
        :type timeout: int
        :param revoke: Whether prior locks should be revoked. Can be set to
                       True to request and wait for prior locks to release
                       their lock, or :obj:`IMMEDIATE` to destroy the blocking
                       write locks and attempt to acquire a read lock.
        :type revoke: bool or :obj:`IMMEDIATE`

        :returns: True if the lock was acquired, False otherwise
        :rtype: bool

        """
        node_name = '/read-'
        self._locks.has_lock = has_read_lock
        return self._acquire_lock(node_name, timeout, revoke)


class ZkWriteLock(_LockBase):
    """Shared Zookeeper Write Lock

    A write-lock is only successful if there are no read or write locks
    active.

    This class takes the same initialization parameters as
    :class:`ZkLock`.

    """
    def acquire(self, timeout=None, revoke=False):
        """Acquire a shared write lock

        :param timeout: How long to wait to acquire the lock, set to 0 to
                        get non-blocking behavior.
        :type timeout: int
        :param revoke: Whether prior locks should be revoked. Can be set to
                       True to request and wait for prior locks to release
                       their lock, or :obj:`IMMEDIATE` to destroy the blocking
                       read/write locks and attempt to acquire a write lock.
        :type revoke: bool or :obj:`IMMEDIATE`

        :returns: True if the lock was acquired, False otherwise
        :rtype: bool

        """
        node_name = '/write-'
        self._locks.has_lock = has_write_lock
        return self._acquire_lock(node_name, timeout, revoke)


def has_read_lock(keyname, children):
    """Determines if this keyname has a valid read lock

    :param keyname: The keyname without full path prefix of the current node
                    being examined
    :type keyname: str
    :param children: List of the children nodes at this lock point
    :type children: list

    """
    prior_nodes = children[:children.index(keyname)]
    prior_write_nodes = [x for x in prior_nodes if \
                         x.startswith('write-')]
    if not prior_write_nodes:
        return True, None
    else:
        return False, prior_write_nodes


def has_write_lock(keyname, children):
    """Determines if this keyname has a valid write lock

    :param keyname: The keyname without full path prefix of the current node
                    being examined
    :type keyname: str
    :param children: List of the children nodes at this lock point
    :type children: list

    """
    if keyname == children[0]:
        return True, None
    return False, children[:children.index(keyname)]


def lock_cli():
    """Zktools Lock CLI"""
    usage = "usage: %prog COMMAND"
    parser = OptionParser(usage=usage)
    parser.add_option("--host", dest="host", type="str",
                      default='localhost:2181',
                      help="Zookeeper host string")
    parser.add_option("--lock_root", dest="lock_root", type="str",
                      default="/ZktoolsLocks", help="Lock root node")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        puts(colored.red("Specify a command: list, remove, or show"))
        return
    command = args[0]
    if command not in ['list', 'remove', 'show']:
        puts(colored.red("Unrecognized command. Valid commands: list, remove, "
                        "show"))
        return

    conn = ZooKeeper(options.host)
    if command == 'list':
        children = conn.get_children(options.lock_root)

        col1, col2 = 30, 70
        puts(columns([colored.cyan("LOCK"), col1],
                     [colored.cyan("STATUS"), col2]))
        for child in children:
            try:
                locks = conn.get_children(options.lock_root + '/' + child)
            except zookeeper.NoNodeException:
                continue
            if locks:
                status = colored.red("Locked")
            else:
                status = colored.green("Free")
            puts(columns([child, col1], [status, col2]))
    elif command == 'remove':
        if len(args) < 2:
            puts(colored.red("You must specify a node to remove."))
            return
        conn.delete(options.lock_root + '/' + args[1])
    elif command == 'show':
        if len(args) < 2:
            puts(colored.red("You must specify a node to show."))
            return
        children = conn.get_children(options.lock_root + '/' + args[1])

        col1, col2, col3 = 20, 15, None
        puts(columns([colored.cyan("LOCK HOLDER"), col1],
                     [colored.cyan("DATA"), col2],
                     [colored.cyan("INFO"), col3]))
        for child in sorted(children):
            try:
                node_name = '%s/%s/%s' % (options.lock_root, args[1],
                                          child)
                value, info = conn.get(node_name)
            except zookeeper.NoNodeException:
                continue
            info['created_ago'] = int(time.time() - (info['ctime'] / 1000))
            info['modifed_ago'] = int(time.time() - (info['mtime'] / 1000))
            puts(columns([child, col1], [value, col2], [str(info), col3]))
