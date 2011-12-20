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
"""Zookeeper Configuration

This module provides a :class:`ZkNode` object which can load itself from
a Zookeeper path, and serialize itself back.

"""
import datetime
import decimal
import re
import threading

import zookeeper

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}


CONVERSIONS = {
    re.compile(r'^\d+\.\d+$'): decimal.Decimal,
    re.compile(r'^\d+$'): int,
    re.compile(r'^true$', re.IGNORECASE): lambda x: True,
    re.compile(r'^false$', re.IGNORECASE): lambda x: False,
    re.compile(r'^None$', re.IGNORECASE): lambda x: None,
    re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$'):
       lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'),
    re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+Z$'):
       lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%fZ'),
    re.compile(r'^\d{4}-\d{2}-\d{2}$'):
       lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'),
}


def _load_value(value):
    """Convert a saved value to the best Python match"""
    for regex, convert in CONVERSIONS.iteritems():
        if regex.match(value):
            return convert(value)
    return value


def _save_value(value):
    """Convert a Python object to the best string repr"""
    # Float is all we care about, as we lose float precision
    # when calling str on it
    if isinstance(value, float):
        return repr(value)
    elif isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    elif isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    else:
        return str(value)


class ZkNode(object):
    """Zookeeper Node

    This object provides access to a single node for updating the
    value that can also track changes to the value from Zookeeper.

    The value of the node is coerced into an appropriate Python
    object when loaded, current supported conversions::

        Numbers with decimals      -> Decimal
        Numbers without decimals   -> Int
        true/false                 -> Bool
        none                       -> None
        ISO 8601 Date and Datetime -> date or datetime

    Example::

        from zktools.connection import ZkConnection
        from zktools.configuration import ZkNode

        conn = ZkConnection()
        node = ZkNode(conn, '/some/config/node', load=True)
        print node.value  # prints out the current value
        node.set(483.24)  # Update the value in zookeeper


    The default behavior is to track changes to the node, so that
    the ``value`` attribute always reflects the node's value in
    Zookeeper.

    .. warning::

        It's advised to periodically check the ``deleted``
        attribute if your particular use-case allows for configuration
        nodes to be deleted.

    """
    def __init__(self, connection, path, track_changes=True, load=False):
        """Create a Zookeeper Node

        Creating a ZkNode does not touch Zookeeper, the other instance
        methods should be used to load an existing value if one is
        expected, or create it.

        In the event the node is deleted once this object is deleted
        once its being tracked, the ``deleted`` attribute will be
        ``True``.

        :param connection: zookeeper connection object
        :type connection: ZkConnection instance
        :param path: Path to the Zookeeper node
        :type path: str
        :param track_changes: Whether the node should set a watch
                              to auto-udpate itself when a change
                              occurs
        :type track_changes: bool
        :param load: Load the value from the node immediately? If set to
                     True then the :meth:`load` method will be called
                     immediately
        :type load: bool

        """
        self._zk = connection
        self._path = path
        self._track_changes = track_changes
        self._object_state = {}
        self._cv = threading.Condition()

        # Public attributes
        self.value = None
        self.deleted = False

        if load:
            self.load()

    def _node_watcher(self, handle, type, state, path):
        """Watch a node for updates"""
        if not self._track_changes:
            return

        self._cv.acquire()
        if type == zookeeper.CHANGED_EVENT:
            data, info = self._zk.get(self._path, self._node_watcher)
            self._object_state.update(info)
            self.value = _load_value(data)
        elif type == zookeeper.DELETED_EVENT:
            self.deleted = True
            self.value = None
            self._object_state = {}
        self._cv.release()

    def create(self, value=None):
        """Create the node in Zookeeper

        An exception will be tossed if the node can't be created due
        to the path to it not existing.

        :param value: The value of the node
        :type value: Any str'able object

        :returns: True if the node was created, False if the
                  node already exists.
        :rtype: bool

        """
        try:
            self._zk.create(self._path, _save_value(value),
                           [ZOO_OPEN_ACL_UNSAFE], 0)
        except zookeeper.NodeExistsException:
            return False
        except zookeeper.NoNodeException:
            raise Exception("Unable to create node: %s, perhaps "
                            "the path to the node doesn't exist?" %
                            self._path)

        # Node is created, lets update ourself to ensure we're
        # still current
        self.load()
        return True

    def load(self):
        """Load data from the node, and coerce as necessary"""
        self._cv.acquire()
        data, info = self._zk.get(self._path, self._node_watcher)
        self._object_state.update(info)
        self.value = _load_value(data)
        self._cv.release()

    def set(self, value):
        """Set the value with a new one

        :param value: The value of the node
        :type value: Any str'able object

        """
        if self.deleted:
            raise Exception("Can't update this node, it has been "
                            "deleted. You must call create first to "
                            "recreate it.")
        self._zk.set(self._path, _save_value(value))
