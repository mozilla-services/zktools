# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Mozilla zktools.
#
# The Initial Developer of the Original Code is Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2011
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
# Ben Bangert (bbangert@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
"""Zookeeper Locking

This module provides a :class:`ZkLock`, which should look familiar to anyone
that has used Python's ``threading.Lock`` class. In addition to normal locking
behavior, revokable shared read/write locks with are also supported. Both the
:class:`ZkLock` and :class:`SharedZkLock` can be revoked as desired. This
requires the current lock holder(s) to release their lock(s).

Shared Read/Write Locks
=======================

Also known in the Zookeeper Recipes as ``Revocable Shared Locks with Freaking
Laser Beams``, :class:`SharedZkLock`'s support read and write locks. A read
lock can be acquired as long as no write locks are active, while a write-lock
can only be acquired when there are no other read or write locks active.

"""
import logging
import threading
import time

import zookeeper


ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}
IMMEDIATE = object()

log = logging.getLogger(__name__)


class _LockBase(object):
    """Base lock implementation for subclasses"""
    def __init__(self, connection, lock_name, lock_root='/ZktoolsLocks',
                 logfile=None):
        """Create a Zookeeper lock object

        :param connection: zookeeper connection object
        :type connection: ZkConnection instance
        :param lock_root: Path to the root lock node to create the locks
                          under
        :type lock_root: string
        :param logfile: Path to a file to log the zookeeper stream to
        :type logfile: string

        """
        self.zk = connection
        self.cv = threading.Condition()
        self.lock_root = lock_root
        self.locks = threading.local()
        self.locks.revoked = []
        self.log_debug = logging.DEBUG >= log.getEffectiveLevel()
        if logfile:
            zookeeper.set_log_stream(open(logfile))
        else:
            zookeeper.set_log_stream(open("/dev/null"))

        # Ensure out lock dir exists
        try:
            self.zk.create(self.lock_root, "zktools ZLock dir",
                           [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            if self.log_debug:
                log.debug("Lock node in zookeeper already created")

        # Try and create our locking node
        self.locknode = '%s/%s' % (self.lock_root, lock_name)
        try:
            self.zk.create(self.locknode, "lock", [ZOO_OPEN_ACL_UNSAFE], 0)
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
        :type revoke: bool or :obj:``


        :returns: True if the lock was acquired, False otherwise
        :rtype: bool

        """
        revoke_lock = self.locks.revoked

        def revoke_watcher(handle, type, state, path):
            if type == zookeeper.CHANGED_EVENT:
                data = self.zk.get(path, revoke_watcher)[0]
                if data == 'unlock':
                    revoke_lock.append(True)

        # Create a lock node
        znode = self.zk.create(self.locknode + node_name, "0",
                               [ZOO_OPEN_ACL_UNSAFE],
                               zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
        data = self.zk.get(znode, revoke_watcher)[0]
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
                        self.zk.delete(znode)
                    except zookeeper.NoNodeException:
                        pass
                    return False

            # Get all the children of the node
            children = self.zk.get_children(self.locknode)
            children.sort(key=lambda val: val[val.rfind('-') + 1:])

            if len(children) == 0 or not keyname in children:
                # Disconnects or other errors can cause this
                znode = self.zk.create(
                    self.locknode + node_name, "0", [ZOO_OPEN_ACL_UNSAFE],
                    zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
                keyname = znode[znode.rfind('/') + 1:]
                data = self.zk.get(znode, revoke_watcher)[0]
                if data == 'unlock':
                    revoke_lock.append(True)
                continue
            acquired, blocking_nodes = self.locks.has_lock(keyname, children)
            if acquired:
                break

            if revoke == IMMEDIATE:
                # Remove all prior nodes
                for node in blocking_nodes:
                    try:
                        self.zk.delete(self.locknode + '/' + node)
                    except zookeeper.NoNodeException:
                        pass
                continue  # Now try again
            elif revoke:
                # Ask all prior nodes to release
                for node in blocking_nodes:
                    try:
                        self.zk.set(self.locknode + '/' + node, "unlock")
                    except zookeeper.NoNodeException:
                        pass
            prior_blocking_node = self.locknode + '/' + blocking_nodes[-1]
            exists = self.zk.exists(prior_blocking_node, lock_watcher)
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
        self.locks.lock_node = znode
        return True

    def release(self):
        """Release a lock

        :returns: True if the lock was released, or False if it is no
                  longer valid.
        :rtype: bool

        """
        self.locks.revoked = []
        try:
            self.zk.delete(self.locks.lock_node)
            del self.locks.lock_node
            return True
        except (zookeeper.NoNodeException, AttributeError):
            return False

    def revoked(self):
        """Indicate if this shared lock has been revoked

        :returns: True if the lock has been revoked, False otherwise.
        :rtype: bool
        """
        return bool(self.locks.revoked)

    def has_lock(self):
        """Check with Zookeeper to see if the lock is acquired

        :returns: Whether the lock is acquired or not
        :rtype: bool

        """
        znode = self.locks.lock_node
        keyname = znode[znode.rfind('/') + 1:]
        # Get all the children of the node
        children = self.zk.get_children(self.locknode)
        children.sort(key=lambda val: val[val.rfind('-') + 1:])
        if keyname not in children:
            return False

        acquired = self.locks.has_lock(keyname, children)[0]
        return bool(acquired)

    def clear(self):
        """Clear out a lock

        .. warning::

            You must be sure this is a dead lock, as clearing it will
            forcably release it.

        :returns: True if the lock was cleared, or False if it
                  is no longer valid.
        :rtype: bool

        """
        children = self.zk.get_children(self.locknode)
        for child in children:
            try:
                self.zk.delete(self.lock_root + '/' + child)
            except zookeeper.NoNodeException:
                pass


class ZkLock(_LockBase):
    """Zookeeper Lock

    Implements a Zookeeper based lock optionally with lock revocation
    should locks be idle for more than a specific set of time.

    Example::

        from zktools.connection import ZkConnection
        from zktools.locking import ZkLock

        # Create a connection and a lock
        conn = ZkConnection()
        my_lock = ZkLock(conn, "my_lock_name")

        my_lock.acquire() # wait to acquire lock
        # do something with the lock

        my_lock.release() # release our lock

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
        self.locks.has_lock = has_write_lock
        return self._acquire_lock(node_name, timeout, revoke)


class SharedZkLock(_LockBase):
    """Shared Zookeeper Lock

    SharedZkLock's can be either write-locks or read-locks. A read-lock
    is considered succesful if there are no active write locks. A
    write-lock is only succesful if there are no read or write locks
    active.

    This class takes the same initialization parameters as
    :class:`ZkLock`.

    """
    def acquire_read_lock(self, timeout=None, revoke=False):
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
        self.locks.has_lock = has_read_lock
        return self._acquire_lock(node_name, timeout, revoke)

    def acquire_write_lock(self, timeout=None, revoke=False):
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
        self.locks.has_lock = has_write_lock
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
