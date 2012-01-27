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
import time
import threading

import zookeeper


class ZkConnection(object):
    """Zookeeper Connection object"""
    def __init__(self, host="localhost:2181", connect_timeout=10,
                 session_timeout=10 * 1000, reconnect_timeout=10 * 1000,
                 debug=False):
        """Create a connection object, capable of automatically
        reconnecting when the connection is lost.

        In the event the connection is lost, the zookeeper command
        will block until the connection is available again, or the
        ``reconnect_timeout`` is reached. The latter will raise an
        Exception about the reconnect period expiring.

        Example::

            conn = ZkConnection()
            conn.connect()

            node = conn.create(
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
        :param reconnect_timeout: How many seconds to attempt to
                                  reconnect before giving up. Set to 0
                                  to avoid reconnect attempts.
        :type reconnect_timeout: int

        """
        self.connected = False
        self._establish_connection = False
        self._host = host
        self._connect_timeout = connect_timeout
        self._session_timeout = session_timeout
        self._reconnect_timeout = reconnect_timeout
        self._cv = threading.Condition()
        self._handle = None
        if not debug:
            zookeeper.set_log_stream(open("/dev/null"))

    def _handle_connection(self, handle, typ, state, path):
        # The Zookeeper API runs this in a separate event thread
        with self._cv:
            if typ == zookeeper.SESSION_EVENT:
                if state == zookeeper.CONNECTED_STATE:
                    self.connected = True
                elif state == zookeeper.CONNECTING_STATE:
                    self.connected = False
                elif state in (zookeeper.EXPIRED_SESSION_STATE,
                               zookeeper.AUTH_FAILED_STATE):
                    # Last event for this connection, session is dead,
                    # clean up
                    self._handle = None
                    self.connected = False
            self._cv.notify_all()

    def connect(self):
        """Connect to zookeeper"""
        # Run this with our threading Condition
        self._establish_connection = True
        with self._cv:
            # First, check that we didn't just connect
            if self.connected:
                return

            # See if the client is already attempting a connection
            if self._handle is not None:
                start_time = time.time()
                time_taken = 0
                while time_taken <= self._reconnect_timeout:
                    # Release and wait until we hit our timeout
                    # NOTE: We loop here, because the connection handler
                    # releases for *every state change*, and we only care
                    # about getting connected
                    self._cv.wait(self._reconnect_timeout - time_taken)

                    if self.connected:
                        return

                    # Triggered, if the session is expired, we break out
                    # to let logic continue below
                    if self._handle is None:
                        break
                    time_taken = time.time() - start_time

                if self._handle is not None and not self.connected:
                    raise Exception("Timed out waiting for reconnect.")

            # Either first run, or a prior session was ditched entirely
            self._handle = zookeeper.init(self._host, self._handle_connection,
                                         self._session_timeout)
            self._cv.wait(self._connect_timeout)
            if not self.connected:
                raise Exception("Unable to connect to Zookeeper")

    def __getattr__(self, name):
        """Returns a reconnecting version that also uses the current handle"""
        zoo_func = getattr(zookeeper, name)

        def call_func(*args, **kwargs):
            # We wait/try this until we're connected, unless it took
            # too long
            if name == 'close':
                self._establish_connection = False

            # Check that we're still connected
            if not self.connected and self._establish_connection:
                self.connect()

            start_time = time.time()
            time_taken = 0
            while time_taken <= self._reconnect_timeout:
                try:
                    return zoo_func(self._handle, *args, **kwargs)
                except (zookeeper.ConnectionLossException,
                        zookeeper.SessionExpiredException,
                        zookeeper.SessionMovedException):
                    if not self._establish_connection:
                        raise
                    self.connect()
                except zookeeper.ZooKeeperException, msg:
                    if 'zhandle already freed' in msg:
                        self._handle = None
                        self.connected = False
                        self.connect()
                    else:
                        raise
                time_taken = time.time() - start_time
            raise Exception("Unable to reconnect to execute command.")
        call_func.__doc__ = zoo_func.__doc__

        # Set this function on ourself so that further calls bypass
        # getattr
        setattr(self, name, call_func)
        return call_func

    def __dir__(self):
        return self.__dict__.keys() + zookeeper.__dict__.keys()
