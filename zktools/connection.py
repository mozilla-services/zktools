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
"""Zookeeper Connection Classes"""
import threading

import zookeeper


class ZkConnection(object):
    """Zookeeper Connection object"""
    def __init__(self, host="localhost:2181", connect_timeout=10,
                 session_timeout=10 * 1000, reconnect=True):
        """Create a connection object

        Example::

            zk = ZkConnection()
            zk.connect()

            node = zk.create(
                "/my/node", "a value",
                [{"perms": 0x1f, "scheme": "world", "id": "anyone"}],
                0)

        .. note::

            ZkConnection ensures that all zookeeper functions use the
            same zookeeper handle that ZkConnection has, so it does
            not need to be passed in as the first argument.

        :param host: A valid zookeeper host string
        :param connect_timeout: Timeout for connecting to zookeeper
        :type connect_timeout: int
        :param session_timeout: Timeout for the zookeeper session
        :type session_timeout: int
        :param reconnect: Whether the connection should automatically
                          be re-established when it goes down.
        :type reconnect: bool

        """
        self.connected = False
        self.host = host
        self.connect_timeout = connect_timeout
        self.session_timeout = session_timeout
        self.reconnect = reconnect
        self.cv = threading.Condition()
        self.handle = None

    def connect(self):
        """Connect to zookeeper"""
        self.cv.acquire()

        # See if the connection was already established
        if self.handle and zookeeper.state(self.handle) == zookeeper.CONNECTED_STATE:
            return

        def handle_connection(handle, typ, state, path):
            self.cv.acquire()
            if typ == zookeeper.SESSION_EVENT:
                if state == zookeeper.CONNECTED_STATE:
                    self.connected = True
                elif state == zookeeper.CONNECTING_STATE:
                    self.connected = False
            self.cv.notify()
            self.cv.release()
        self.handle = zookeeper.init(self.host, handle_connection,
                                     self.session_timeout)
        self.cv.wait(self.connect_timeout)
        if not self.connected:
            raise Exception("Unable to connect to Zookeeper")
        self.cv.notify_all()
        self.cv.release()

    def __getattr__(self, name):
        """Returns a reconnecting version that also uses the current handle"""
        zoo_func = getattr(zookeeper, name)

        # Check that we're still connected
        if not self.connected and self.reconnect:
            self.connect()

        def call_func(*args, **kwargs):
            if not self.reconnect:
                return zoo_func(self.handle, *args, **kwargs)
            try:
                return zoo_func(self.handle, *args, **kwargs)
            except (zookeeper.ConnectionLossException,
                    zookeeper.SessionExpiredException,
                    zookeeper.SessionMovedException):
                self.connect()
                return zoo_func(self.handle, *args, **kwargs)
        call_func.__doc__ = zoo_func.__doc__

        # Set this function on ourself so that further calls bypass
        # getattr
        setattr(self, name, call_func)
        return call_func

    def __dir__(self):
        return self.__dict__.keys() + zookeeper.__dict__.keys()
