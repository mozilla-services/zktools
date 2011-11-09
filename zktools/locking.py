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
import threading

import zookeeper

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}


class Lock(object):
    def __init__(self, hosts, lock_node='/ZktoolsLocks', logfile=None,
                 connect_timeout=10):
        """Create a lock object

        :param hosts: zookeeper hosts
        :hosts type: string
        :param lock_node: Path to the parent lock node to create the locks
                          under
        :lock_node type: string
        :param logfile: Path to a file to log the zookeeper stream to
        :logfile type: string
        :param connect_timeout: Time to wait for zookeeper to connect
        :connect_timeout type: int

        """
        self.hosts = hosts
        self.lock_node = lock_node
        self.connected = threading.Event()
        if logfile:
            zookeeper.set_log_stream(open(logfile))
        else:
            zookeeper.set_log_stream(open("/dev/null"))

        def handle_connection(handle, type, state, path):
            print "connected to zookeeper"
            self.connected.set()
        self.handle = zookeeper.init(hosts, handle_connection,
                                     connect_timeout * 1000)
        self.connected.wait(connect_timeout)

    def acquire(lock_name, blocking=True):
        """Acquire a lock

        :param blocking: Whether to block until a lock can be acquired, or
                         release immediately with False in the event it
                         can't be acquired.
        :blocking type: bool

        """
