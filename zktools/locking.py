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
"""Zookeeper Locking"""
import logging
import threading
import time

import zookeeper


ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}

log = logging.getLogger(__name__)


class ZkLock(object):
    """Zookeeper Lock

    Implements a Zookeeper based lock optionally with lock revokation
    should locks be idle for more than a specific set of time.

    Example::

        from zktools.connection import ZkConnection
        from zktools.locking import ZkLock

        # Create and open the connection
        conn = ZkConnection()
        conn.connect()

        my_lock = ZkLock(conn, "my_lock_name")

        my_lock.acquire() # wait to acquire lock
        # do something with the lock

        my_lock.release() # release our lock

    """
    def __init__(self, connection, lock_name, lock_timeout=None,
                 lock_root='/ZktoolsLocks', logfile=None):
        """Create a Zookeeper lock object

        This implements "Revocable Shared Locks with Freaking Laser Beams"
        when lock_timeout is set, and anyone holding a lock longer than this
        value will have it revoked.

        :param hosts: zookeeper connection object
        :param lock_root: Path to the root lock node to create the locks
                          under
        :lock_root type: string
        :param logfile: Path to a file to log the zookeeper stream to
        :logfile type: string
        :param lock_timeout: Setting a lock_timeout makes this lock
                             revokable and it will be considered invalid
                             after this timeout

        """
        self.zk = connection
        self.cv = threading.Condition()
        self.lock_root = lock_root
        self.lock_timeout = lock_timeout
        self.locks = threading.local()
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

    def acquire(self, timeout=None):
        """Acquire a lock

        :param lock_name: The name of the lock to acquire.
        :param timeout: How long to wait to acquire the lock, set to 0 to
                        get non-blocking behavior.
        :timeout type: int

        """
        # Create a lock node
        znode = self.zk.create(self.locknode + '/lock', "0",
                               [ZOO_OPEN_ACL_UNSAFE],
                               zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
        keyname = znode[znode.rfind('/') + 1:]

        acquired = False
        cv = threading.Event()

        def lock_watcher(handle, type, state, path):
            cv.set()

        expire_times = [x for x in [timeout, self.lock_timeout] if x is not None]
        if expire_times:
            wait_for = min(expire_times)
        else:
            wait_for = None

        lock_start = time.time()
        while not acquired:
            cv.clear()

            # Have we been at this longer than the timeout?
            if timeout is not None and time.time() - lock_start > timeout:
                try:
                    self.zk.delete(znode)
                except:
                    pass
                return False

            # Get all the children of the node
            children = self.zk.get_children(self.locknode)
            children.sort()

            if len(children) == 0 or not keyname in children:
                # Disconnects or other errors can cause this
                znode = self.zk.create(
                    self.locknode + '/lock', "0", [ZOO_OPEN_ACL_UNSAFE],
                    zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
                keyname = znode[znode.rfind('/') + 1:]
                continue

            if keyname == children[0]:
                # The lock is ours, make sure to touch it so
                # that we don't get revoked too soon
                try:
                    self.zk.set(znode, "0")
                except zookeeper.NoNodeException:
                    # Weren't able to update our node cause its gone
                    znode = self.zk.create(
                        self.locknode + '/lock', "0", [ZOO_OPEN_ACL_UNSAFE],
                        zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
                    keyname = znode[znode.rfind('/') + 1:]
                    continue
                acquired = True
                break

            if self.lock_timeout:
                # Revocable Shared Locks with Freaking Laser Beams impl.
                data, info = self.zk.get(self.locknode + '/' + children[0])
                mtime = info['mtime'] / 1000
                if time.time() - mtime > self.lock_timeout:
                    # First node we found is expired, remove it and repeat
                    # We pass in the version to ensure it didn't just get
                    # updated
                    try:
                        self.zk.delete(self.locknode + '/' + children[0],
                                       info['version'])
                    except:
                        # If the delete fails, it got changed, thats fine.
                        pass
                    continue

            # Set a watch on the next lowest node in the sequence
            # This avoids the herd effect (Everyone watching for changes
            # on the parent node)
            prior_node = children[children.index(keyname) - 1]
            prior_node = self.locknode + '/' + prior_node
            exists = self.zk.exists(prior_node, lock_watcher)

            if not exists:
                # The node disappeared? Rinse and repeat.
                continue

            # Wait for a notification from get_children, no longer
            # than the expires time or the timeout, or wait until
            # the watch triggers if neither expires nor timeout was
            # set
            cv.wait(wait_for)
        self.locks.lock_node = znode
        return True

    def release(self):
        """Release a lock

        Returns True if the lock was released, or False if it is no
        longer valid.

        """
        try:
            self.zk.delete(self.locks.lock_node)
            return True
        except:
            return False

    def renew(self):
        """Renews an existing lock

        Used to renew a lock when using revokable shared locks.

        Returns True if the lock was renewed, or False if it is no
        longer valid.

        """
        try:
            self.zk.set(self.locks.lock_node, "0")
            return True
        except:
            return False

    def has_lock(self):
        """Returns whether the lock is acquired or not"""
        try:
            znode = self.locks.lock_node
            keyname = znode[znode.rfind('/') + 1:]
            # Get all the children of the node
            children = self.zk.get_children(self.locknode)
            children.sort()

            if children and children[0] == keyname:
                return True
        except:
            pass
        return False

    def clear(self):
        """Clear out a lock

        .. warning::

            You must be sure this is a dead lock, as clearing it will
            forcably release it.

        Returns True if the lock was cleared, or False if it
        is no longer valid.

        """
        children = self.zk.get_children(self.locknode)
        for child in children:
            self.zk.delete(self.lock_root + '/' + child)
