# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Zookeeper Nodes

This module provides a :class:`ZkNode` object which can represent a single
node from Zookeeper. It can reflect a single value, or a JSON serialized
value.

"""
import datetime
import decimal
import json
import re
import threading

import zookeeper

ZOO_OPEN_ACL_UNSAFE = dict(perms=zookeeper.PERM_ALL, scheme='world',
                           id='anyone')


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

JSON_REGEX = re.compile(r'^[\{\[].*[\}\]]$')

# Sad fix for http://bugs.python.org/issue7980
# We import and use strptime here to ensure the whole chain is fully
# imported before threading is likely to occur which remedies the bug
assert datetime.datetime.strptime('2004-02-03T12:10:32.4Z',
                                  '%Y-%m-%dT%H:%M:%S.%fZ')


def _load_value(value, use_json=False):
    """Convert a saved value to the best Python match"""
    if use_json and JSON_REGEX.match(value):
        try:
            return json.loads(value)
        except ValueError:
            return value
    for regex, convert in CONVERSIONS.iteritems():
        if regex.match(value):
            return convert(value)
    return value


def _save_value(value, use_json=False):
    """Convert a Python object to the best string repr"""
    # Float is all we care about, as we lose float precision
    # when calling str on it
    if isinstance(value, float):
        return repr(value)
    elif isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    elif isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    elif use_json and isinstance(value, (dict, list)):
        return json.dumps(value)
    else:
        return str(value)


class ZkNode(object):
    """Zookeeper Node

    This object provides access to a single node for updating the value
    that can also track changes to the value from Zookeeper. Functions
    can be subscribed to changes in the node's value and/or changes in
    the node's children (nodes under it being created/removed).

    The value of the node is coerced into an appropriate Python
    object when loaded, current supported conversions::

        Numbers with decimals      -> Decimal
        Numbers without decimals   -> Int
        true/false                 -> Bool
        none                       -> None
        ISO 8601 Date and Datetime -> date or datetime

    And optionally, with use_json::

        JSON string                -> dict/list

    .. note::

        The JSON determination is extremely lax, if it is a string that
        starts and ends with brackets or curly marks, it is assumed to
        be a JSON object and will be coerced if possible. If coercion
        fails, the string will be returned as is.

    Example::

        from zc.zk import ZooKeeper
        from zktools.node import ZkNode

        conn = ZooKeeper()
        node = ZkNode(conn, '/some/config/node')

        # prints out the current value, defaults to None
        print node.value

        # Set the value in zookeeper
        node.value = 483.24

        # Show the children of the node
        print list(node.children)

        # Subscribe a function to be called when the node's
        # children change (note this will be called immediately
        # with a zc.zk Children instance)
        @node.children
        def my_function(children):
            # do something with node.children or prior_children

    The default behavior is to track changes to the node, so that
    the ``value`` attribute always reflects the node's value in
    Zookeeper. Additional subscriber functions are called when the
    Zookeeper event watch is triggered and are run in a separate
    event thread. Depending on how fast the value/children are
    changing the subscriber functions may run consecutively and
    could miss intermediate values.

    Return values of subscriber functions are ignored.

    .. warning::

        **Do not delete nodes that are in use**, there intentionally is
        no code to handle such conditions as it creates overly complex
        scenarios both for ZkNode and for application code using it.

    """
    def __init__(self, connection, path, default=None, use_json=False,
                 permission=ZOO_OPEN_ACL_UNSAFE, create_mode=0):
        """Create a Zookeeper Node

        Creating a ZkNode by default attempts to load the value, and
        if it is not found will automatically create a blank string as
        the value.

        The last time a :class:`ZkNode` has been modified either by
        the user or due to a Zookeeper update is recorded as the
        :obj:`ZkNode.last_modified` attribute, as a long in
            milliseconds from epoch.

        :param connection: Zookeeper connection object
        :type connection: zc.zk Zookeeper instance
        :param path: Path to the Zookeeper node
        :type path: str
        :param default: A default value if the node is being created
        :param use_json: Whether values that look like a JSON object should
                         be deserialized, and dicts/lists saved as JSON.
        :type use_json: bool
        :param permission: Node permission to use if the node is being
                           created.
        :type permission: dict
        :param create_mode: Persistent or ephemeral creation mode
        :type create_mode: int
        """
        self._zk = connection
        self._path = path
        self._cv = threading.Condition()
        self._use_json = use_json
        self._value = None
        self._reload_data = False

        if not connection.exists(path):
            self._zk.create(self._path,
                            _save_value(default, use_json=use_json),
                            [permission], create_mode)
        self._load()

    def _node_watcher(self, handle, type, state, path):
        """Watch a node for updates"""
        if type == zookeeper.CHANGED_EVENT:
            data = self._zk.get(self._path, self._node_watcher)
            self.last_modified = data[1][u'mtime']
            self._value = _load_value(data[0], use_json=self._use_json)
        elif type in (zookeeper.EXPIRED_SESSION_STATE,
                      zookeeper.AUTH_FAILED_STATE):
            self._reload_data = True

    def _load(self):
        """Load data from the node, and coerce as necessary"""
        data = self._zk.get(self._path, self._node_watcher)
        self.last_modified = data[1][u'mtime']
        self._value = _load_value(data[0], use_json=self._use_json)

    @property
    def value(self):
        """Returns the current value

        If the Zookeeper session expired, it will be reconnected and
        the value reloaded.

        """
        if self._reload_data:
            self._load()
            self._reload = False
        return self._value

    @value.setter
    def value(self, value):
        """Set the value to a new one

        :param value: The value of the node
        :type value: Any str'able object

        """
        self._value = val = _save_value(value, use_json=self._use_json)
        self._zk.set(self._path, val)

    @property
    def connected(self):
        """Indicate whether a connection to Zookeeper exists"""
        return self._zk.connected
