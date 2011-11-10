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


class ZConnection(object):
    def __init__(self, host="localhost:2181", connect_timeout=10,
                 session_timeout=10 * 1000, reconnect=True):
        self.connected = False
        self.host = host
        self.connect_timeout = connect_timeout
        self.session_timeout = session_timeout
        self.reconnect = reconnect
        self.cv = threading.Condition()
        self.handle = None

    def connect(self):
        if not self.cv.acquire(blocking=False):
            # We don't have the lock, wait for the connection to come up
            self.cv.wait(self.connect_timeout)

            # Is the connection usable now?
            if zookeeper.state(self.handle) == zookeeper.CONNECTED_STATE:
                return
            else:
                raise Exception("Zookeeper connection not re-established")
        self.connected = False

        def handle_connection(handle, type, state, path):
            self.cv.acquire()
            self.cv.notify()
            self.connected = True
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

        def call_func(*args, **kwargs):
            if not self.reconnect:
                return zoo_func(self.handle, *args, **kwargs)
            try:
                return zoo_func(self.handle, *args, **kwargs)
            except zookeeper.ConnectionLossException:
                self.connect()
                return zoo_func(self.handle, *args, **kwargs)
        return call_func
