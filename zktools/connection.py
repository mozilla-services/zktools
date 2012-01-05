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
                 session_timeout=10 * 1000, reconnect=True,
                 reconnect_timeout=10 * 1000):
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
        self._host = host
        self._connect_timeout = connect_timeout
        self._session_timeout = session_timeout
        self._reconnect = reconnect
        self._reconnect_timeout = reconnect_timeout
        self._cv = threading.Condition()
        self._handle = self._session_id = None
        self._connection_in_progress = False

    def _handle_connection(self, handle, typ, state, path):
        self._cv.acquire()
        if typ == zookeeper.SESSION_EVENT:
            if state == zookeeper.CONNECTED_STATE:
                print "Connected\n"
                self.connected = True
                self._session_id = zookeeper.client_id(handle)
            elif state == zookeeper.CONNECTING_STATE:
                print "Connecting....\n"
                self.connected = False
                self._connection_in_progress = True
            elif state in (zookeeper.EXPIRED_SESSION_STATE,
                           zookeeper.AUTH_FAILED_STATE):
                # Last event for this connection, session is dead,
                # clean up
                self.connected = False
                self._connection_in_progress = False
        self._cv.notify_all()
        self._cv.release()

    def connect(self):
        """Connect to zookeeper"""
        self._cv.acquire()

        # See if the client is already attempting a connection
        if self._connection_in_progress:
            self._cv.wait(self._reconnect_timeout)
            if not self.connected:
                self._cv.release()
                raise Exception("Timed out waiting for reconnect.")
            return

        # See if the connection was already established
        if self._handle is not None:
            if zookeeper.state(self._handle) == zookeeper.CONNECTED_STATE:
                return

        # Were we previously established? Try and reconnect to that
        # session
        if self._session_id:
            print "try previous session id"
            try:
                self._handle = zookeeper.init(
                    self._host,
                    self._handle_connection,
                    self._session_timeout,
                    self._session_id
                )
                self._cv.wait(self._reconnect_timeout)
            except (zookeeper.SessionExpiredException,
                    zookeeper.SessionMovedException):
                print "failed re-establishing"
                self._session_id = None
                self._cv.release()
                self.connect()
        else:
            self._handle = zookeeper.init(self._host, self._handle_connection,
                                         self._session_timeout)
            self._cv.wait(self._connect_timeout)
        self._cv.release()
        if self.connected:
            print "managed to connect"
        if not self.connected:
            raise Exception("Unable to connect to Zookeeper")

    def __getattr__(self, name):
        """Returns a reconnecting version that also uses the current handle"""
        zoo_func = getattr(zookeeper, name)

        # Check that we're still connected
        if not self.connected and self._reconnect:
            self.connect()

        def call_func(*args, **kwargs):
            if not self._reconnect:
                return zoo_func(self._handle, *args, **kwargs)
            try:
                return zoo_func(self._handle, *args, **kwargs)
            except (zookeeper.ConnectionLossException,
                    zookeeper.SessionExpiredException,
                    zookeeper.SessionMovedException):
                self.connect()
                return zoo_func(self._handle, *args, **kwargs)
        call_func.__doc__ = zoo_func.__doc__

        # Set this function on ourself so that further calls bypass
        # getattr
        setattr(self, name, call_func)
        return call_func

    def __dir__(self):
        return self.__dict__.keys() + zookeeper.__dict__.keys()
