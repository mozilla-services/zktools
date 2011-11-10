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
"""locking"""
from collections import defaultdict
import logging
import threading

import zookeeper

from zktools.connection import ZConnection

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}

log = logging.getLogger(__name__)


class ZLock(object):
    def __init__(self, hosts, lock_node='/ZktoolsLocks', logfile=None,
                 connect_timeout=10, session_timeout=10 * 1000):
        """Create a Zookeeper lock object

        :param hosts: zookeeper hosts
        :hosts type: string
        :param lock_node: Path to the parent lock node to create the locks
                          under
        :lock_node type: string
        :param logfile: Path to a file to log the zookeeper stream to
        :logfile type: string
        :param connect_timeout: Time to wait for zookeeper to connect
        :connect_timeout type: int
        :param session_timeout: How long the session to zookeeper should be
                                established before timing out
        :session_timeout type: int

        """
        self.cv = threading.Condition()
        self.lock_node = lock_node
        self.log_debug = logging.DEBUG >= log.getEffectiveLevel()
        self.local = threading.local()
        self.locks = defaultdict(threading.Condition)
        if logfile:
            zookeeper.set_log_stream(open(logfile))
        else:
            zookeeper.set_log_stream(open("/dev/null"))

        self.connection = ZConnection(hosts, connect_timeout=connect_timeout,
                                      session_timeout=session_timeout)
        self.connection.connect()

        # Ensure out lock dir exists
        try:
            self.connection.create(self.lock_node, "zktools ZLock dir",
                                   [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            if self.log_debug:
                log.debug("Lock node in zookeeper already created")

    def acquire(self, lock_name, blocking=True):
        """Acquire a lock

        :param lock_name: The name of the lock to acquire.
        :param blocking: Whether to block until a lock can be acquired, or
                         release immediately with False in the event it
                         can't be acquired.
        :blocking type: bool

        """
        # First, try and create our locking node
        locknode = '%s/%s' % (self.lock_node, lock_name)
        try:
            self.connection.create(locknode, "lock", [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            # Ok if this exists already
            pass

        # Now create a node under the locknode
        znode = self.connection.create(locknode + '/lock', "0",
                                       [ZOO_OPEN_ACL_UNSAFE],
                                       zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
        keyname = znode[znode.rfind('/') + 1:]

        acquired = False
        cv = self.locks[locknode]

        def lock_watcher(handle, type, state, path):
            cv.acquire()
            cv.notify_all()
            cv.release()

        cv.acquire()
        while not acquired:
            # Get all the children of the node
            children = self.connection.get_children(locknode)
            children.sort()

            if len(children) == 0 or not keyname in children:
                # Disconnects or other errors can cause this
                znode = self.connection.create(
                    locknode + '/lock', "0", [ZOO_OPEN_ACL_UNSAFE],
                    zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
                continue

            if keyname == children[0]:
                # The lock is ours!
                acquired = True
            else:
                if not blocking:
                    return False

                # Set a watch on the next lowest node in the sequence
                # This avoids the herd effect (Everyone watching for changes
                # on the parent node)
                prior_node = children[children.index(keyname) - 1]
                prior_node = locknode + '/' + prior_node
                exists = self.connection.exists(prior_node, lock_watcher)

                if not exists:
                    # The node disappeared? Rinse and repeat.
                    continue

                # Wait for a notification from get_children
                cv.wait()
        cv.release()

        # We now have the lock, return a function to release it

        def release():
            self.connection.delete(znode)
        return release
