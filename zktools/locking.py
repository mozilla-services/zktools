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
import uuid
from optparse import OptionParser

from clint.textui import colored
from clint.textui import columns
from clint.textui import puts
from zc.zk import ZooKeeper
import zookeeper

from zktools.util import safe_call
from zktools.util import safe_create_ephemeral_sequence
from zktools.util import threaded

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}
IMMEDIATE = object()

log = logging.getLogger(__name__)


def retryable(d):
    return d in (zookeeper.CONNECTIONLOSS, zookeeper.CLOSING,
                 zookeeper.OPERATIONTIMEOUT)


class ZkAsyncLock(object):
    def __init__(self, connection, lock_path):
        self._zk = connection
        self._lock_path = lock_path
        self._lock_event = threading.Event()
        self._acquired = False
        self._candidate_path = None
        self._wait_timeout = None
        self.errors = []

        try:
            safe_call(self._zk, 'create_recursive', self._lock_path,
                      "zktools ZLock dir", [ZOO_OPEN_ACL_UNSAFE])
        except zookeeper.NodeExistsException:
            pass

    def __enter__(self):
        self.acquire()
        self._lock_event.wait(self._wait_timeout)

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        self._lock_event.wait()
        self._wait_timeout = None

    def wait_for(self, timeout=None):
        self._wait_timeout = timeout
        return self

    def wait_for_acquire(self, timeout=None):
        if not self._node_prefix:
            raise Exception("Lock acquisition is not in process")
        self._lock_event.wait(timeout)
        return self.acquired

    @property
    def acquired(self):
        return self._acquired

    def acquire(self):
        if self.acquired:
            raise Exception("Lock already acquired")

        self._lock_event.clear()
        self._node_prefix = uuid.uuid4().hex
        self._create_candidate()
        return False

    def release(self):
        if not self.acquired:
            raise Exception("Lock not acquired")
        self._lock_event.clear()
        self._delete_candidate()

    def _delete_candidate(self):
        self._zk.adelete(self._candidate_path, -1, self._delete_callback)

    @threaded
    def _delete_callback(self, p, rc):
        if rc in (zookeeper.OK, zookeeper.NONODE):
            self._candidate_path = self._node_prefix = None
            self._acquired = False
            self._lock_event.set()
        elif retryable(rc):
            time.sleep(0.2)
            return self._delete_candidate()
        else:
            self.errors.append((rc, 'Delete callback'))

    def _create_candidate(self):
        self._zk.create(self._lock_path + "/%s-lock-" % self._node_prefix,
                        "0", [ZOO_OPEN_ACL_UNSAFE],
                        zookeeper.EPHEMERAL | zookeeper.SEQUENCE,
                        self._candidate_creation)

    def _candidate_creation(self, p, rc, value):
        """Callback for after the node creation runs"""
        if rc == zookeeper.OK:
            self._candidate_path = value
            return self._acquire()
        elif retryable(rc):
            self._zk.aget_children(self._lock_path, None,
                                   self._check_children_for_prefix)
        else:
            self.errors.append((rc, 'Candidate creation'))
            self._lock_event.set()

    @threaded
    def _check_children_for_prefix(self, p, rc, children):
        """Checks to see during candidate creation errors if the node
        was actually created"""
        if rc == zookeeper.OK:
            for child in children:
                if child.startswith(self._node_prefix):  # Child was created
                    self._candidate_path = self._lock_path + '/' + child
                    return self._acquire()
            # No matching child, recreate the candidate
            self._create_candidate()
        elif retryable(rc):  # Small sleep to avoid CPU hit
            time.sleep(0.2)
            self._zk.aget_children(self._lock_path, None,
                                   self._check_children_for_prefix)
        else:
            self.errors.append((rc, 'Check children for prefix'))
            self._lock_event.set()

    def _acquire(self):
        self._zk.aget_children(self._lock_path, None,
                               self._check_candidate_nodes)

    @threaded
    def _check_candidate_nodes(self, p, rc, children):
        if retryable(rc):  # Small sleep to avoid CPU hit
            time.sleep(0.2)
            return self._acquire()
        elif rc != zookeeper.OK:
            self.errors.append((rc, 'Check candidate nodes'))
            return

        candidate_name = self._candidate_path.split('/')[-1]
        if candidate_name not in children:  # Not in list? start over
            self._candidate_path = None
            return self._create_candidate()

        # Sort by sequence, ignore proceeding UUID hex
        children.sort(key=lambda k: k.split('-')[-1])
        index = children.index(candidate_name)

        if index == 0:  # We're first, lock acquired
            self._acquired = True
            return self._lock_event.set()

        # We're not first, watch the next in line
        prior_node = '/'.join([self._lock_path, children[index - 1]])
        self._zk.aget(prior_node, self._prior_node_watch,
                      self._prior_node_exists)

    def _prior_node_exists(self, p, rc, value, stat):
        if rc == zookeeper.NONODE:
            # No node? Check candidates again
            return self._acquire()
        # Node still exists, wait for the watcher and ignore here

    def _prior_node_watch(self, handle, type, state, path):
        if type != zookeeper.SESSION_EVENT:
            # Retrigger our children check
            self._acquire()


class _LockBase(object):
    """Base lock implementation for subclasses"""
    def __init__(self, connection, lock_name, lock_root='/ZktoolsLocks'):
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
        self._lock_root = lock_root
        self._revoked = []
        self._lock_args = ([], {})
        self._has_lock = has_write_lock
        self._log_debug = logging.DEBUG >= log.getEffectiveLevel()
        self._locknode = '%s/%s' % (self._lock_root, lock_name)
        self._candidate_path = ''
        self._ensure_lock_dir()

    def _ensure_lock_dir(self):
        # Ensure our lock dir exists
        if safe_call(self._zk, 'exists', self._locknode):
            return

        try:
            safe_call(self._zk, 'create', self._lock_root,
                      "zktools ZLock dir", [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            if self._log_debug:
                log.debug("Lock node in Zookeeper already created")

        # Try and create our locking node
        try:
            safe_call(self._zk, 'create', self._locknode, "lock",
                      [ZOO_OPEN_ACL_UNSAFE], 0)
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
        self._revoked = []

        # Create a lock node
        self._candidate_path = znode = safe_create_ephemeral_sequence(
            self._zk, self._locknode + node_name, "0", [ZOO_OPEN_ACL_UNSAFE])

        def revoke_watcher(handle, type, state, path):
            # This method must be in closure scope to ensure that
            # it can append to the thread it is called from
            # to indicate if this particular thread's lock was
            # revoked or removed
            @threaded
            def handle_events():
                # Run separately in a thread because safe_call can block the
                # ZK event thread which would be very bad
                if type == zookeeper.CHANGED_EVENT:
                    data = safe_call(self._zk, 'get', path, revoke_watcher)[0]
                    if data == 'unlock':
                        self._revoked.append(True)
                elif type == zookeeper.DELETED_EVENT or \
                     state == zookeeper.EXPIRED_SESSION_STATE:
                    # Trigger if node was deleted
                    self._revoked.append(True)
            handle_events()

        data = safe_call(self._zk, 'get', znode, revoke_watcher)[0]
        if data == 'unlock':
            self._revoked.append(True)
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
                        safe_call(self._zk, 'delete', znode)
                    except zookeeper.NoNodeException:
                        pass
                    return False
            first_run = False

            # Get all the children of the node
            children = safe_call(self._zk, 'get_children', self._locknode)
            children.sort(key=lambda val: val[val.rfind('-') + 1:])

            if len(children) == 0 or not keyname in children:
                # Disconnects or other errors can cause this
                self._candidate_path = znode = safe_create_ephemeral_sequence(
                    self._zk, self._locknode + node_name, "0",
                    [ZOO_OPEN_ACL_UNSAFE])
                keyname = znode[znode.rfind('/') + 1:]
                data = safe_call(self._zk, 'get', znode, revoke_watcher)[0]
                if data == 'unlock':
                    self._revoked.append(True)
                continue

            acquired, blocking_nodes = self._has_lock(keyname, children)
            if acquired:
                break

            if revoke == IMMEDIATE:
                # Remove all prior nodes
                for node in blocking_nodes:
                    try:
                        safe_call(self._zk, 'delete',
                                  self._locknode + '/' + node)
                    except zookeeper.NoNodeException:
                        pass
                continue  # Now try again
            elif revoke:
                # Ask all prior blocking nodes to release
                for node in blocking_nodes:
                    try:
                        safe_call(self._zk, 'set',
                                  self._locknode + '/' + node, "unlock")
                    except zookeeper.NoNodeException:
                        pass

            prior_blocking_node = self._locknode + '/' + blocking_nodes[-1]
            exists = safe_call(self._zk, 'exists', prior_blocking_node,
                               lock_watcher)
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
        return True

    def __call__(self, *args, **kwargs):
        self._lock_args = (args, kwargs)
        return self

    def __enter__(self):
        args, kwargs = self._lock_args
        self.acquire(*args, **kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        self._lock_args = ([], {})
        self.release()

    def release(self):
        """Release a lock

        :returns: True if the lock was released, or False if it is no
                  longer valid.
        :rtype: bool

        """
        self._revoked = []
        try:
            safe_call(self._zk, 'delete', self._candidate_path)
            return True
        except (zookeeper.NoNodeException, AttributeError):
            return False

    def has_lock(self):
        """Check with Zookeeper to see if the lock is acquired

        :returns: Whether the lock is acquired or not
        :rtype: bool

        """
        if not self._candidate_path:
            # So we can check it even if we released
            return False

        znode = self._candidate_path
        keyname = znode[znode.rfind('/') + 1:]
        # Get all the children of the node
        children = safe_call(self._zk, 'get_children', self._locknode)
        children.sort(key=lambda val: val[val.rfind('-') + 1:])
        if keyname not in children:
            return False

        acquired = self._has_lock(keyname, children)[0]
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
        children = safe_call(self._zk, 'get_children', self._locknode)
        for child in children:
            try:
                safe_call(self._zk, 'delete', self._locknode + '/' + child)
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
        children = safe_call(self._zk, 'get_children', self._locknode)
        if not children:
            return False

        for child in children:
            try:
                safe_call(self._zk, 'set', self._locknode + '/' + child,
                          "unlock")
            except zookeeper.NoNodeException:
                pass
        return True

    @property
    def revoked(self):
        """Indicate if this shared lock has been revoked

        :returns: True if the lock has been revoked, False otherwise.
        :rtype: bool
        """
        return bool(self._revoked)

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
        self._has_lock = has_write_lock
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
        self._has_lock = has_read_lock
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
    prior_write_nodes = [x for x in prior_nodes if '-write-' in x]
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
