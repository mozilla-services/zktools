# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Zookeeper Nodes

This module provides a :class:`ZkNode` object which can load itself from
a Zookeeper path, and serialize itself back.

"""
import datetime
import decimal
import json
import re
import time
import threading
import UserDict

import blinker
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

JSON_REGEX = re.compile(r'^[\{\[].*[\}\]]$')


def _load_value(value, use_json=False):
    """Convert a saved value to the best Python match"""
    for regex, convert in CONVERSIONS.iteritems():
        if regex.match(value):
            return convert(value)
    if use_json and JSON_REGEX.match(value):
        try:
            return json.loads(value)
        except ValueError:
            return value
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

        The JSON determination is extremely lax, if its a string that
        starts and ends with brackets or curley marks, its assumed to
        be a JSON object and will be coerced if possible. If coercion
        fails, the string will be returned as is.

    Example::

        from zktools.connection import ZkConnection
        from zktools.node import ZkNode

        conn = ZkConnection()
        node = ZkNode(conn, '/some/config/node')

        # prints out the current value, defaults to None
        print node.value

        # Set the value in zookeeper
        node.value = 483.24

        # Subscribe a function to be called when the node's
        # children change
        def my_function(node, prior_children):
            # do something with node.children or prior_children
        node.add_children_subscriber(my_function)

        # Subscribe a function to be called when the node's
        # data changes
        def my_data_func(node, prior_value):
            # do something with the new value of node.value
        node.add_data_subscriber(my_data_func)

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
                 permission=ZOO_OPEN_ACL_UNSAFE):
        """Create a Zookeeper Node

        Creating a ZkNode by default attempts to load the value, and
        if its not found will automatically create a blank string as
        the value.

        The last time a :class:`ZkNode` has been modified either by
        the user or due to a Zookeeper update is recorded as the
        :obj:`ZkNode.last_modified` attribute.

        :param connection: zookeeper connection object
        :type connection: ZkConnection instance
        :param path: Path to the Zookeeper node
        :type path: str
        :param default: A default value if the node is being created
        :param use_json: Whether values that look like a JSON object should
                         be deserialized, and dicts/lists saved as JSON.
        :type use_json: bool
        :param permission: Node permission to use if the node is being
                           created.
        :type permission: dict

        """
        self._zk = connection
        self._path = path
        self._cv = threading.Condition()
        self._use_json = use_json
        self._value = None
        self._reload_data = self._reload_children = False
        self._children = []
        self.last_modified = time.time()
        # Signals
        self.children_change = blinker.Signal()
        self.data_change = blinker.Signal()

        with self._cv:
            if not connection.exists(path, self._created_watcher):
                self._zk.create(self._path,
                                _save_value(default, use_json=use_json),
                                [permission], 0)

                # Wait for the node to actually be created
                self._cv.wait()
            self._load()
            self._load_children()

    def _created_watcher(self, handle, type, state, path):
        """Watch for our node to be created before continuing"""
        with self._cv:
            if type == zookeeper.CREATED_EVENT:
                self._cv.notify_all()

    def _node_watcher(self, handle, type, state, path):
        """Watch a node for updates"""
        with self._cv:
            if type == zookeeper.CHANGED_EVENT:
                prior_value = self._value
                data = self._zk.get(self._path, self._node_watcher)[0]
                self._value = _load_value(data, use_json=self._use_json)
                self.data_change.send(self, prior_value=prior_value)
                self.last_modified = time.time()
            elif type in (zookeeper.EXPIRED_SESSION_STATE,
                          zookeeper.AUTH_FAILED_STATE):
                self._reload_data = True
            self._cv.notify_all()

    def _children_watcher(self, handle, type, state, path):
        """Watch a node for children changes"""
        with self._cv:
            if type == zookeeper.CHILD_EVENT:
                prior_children = self._children
                self._children = self._zk.get_children(
                    self._path, self._children_watcher)
                self.children_change.send(self, prior_children=prior_children)
                self.last_modified = time.time()
            elif type in (zookeeper.EXPIRED_SESSION_STATE,
                          zookeeper.AUTH_FAILED_STATE):
                self._reload_children = True
            self._cv.notify_all()

    def _load(self):
        """Load data from the node, and coerce as necessary"""
        with self._cv:
            data = self._zk.get(self._path, self._node_watcher)[0]
            self._value = _load_value(data, use_json=self._use_json)

    def _load_children(self):
        """Load children from the node"""
        with self._cv:
            self._children = self._zk.get_children(
                self._path, self._children_watcher)

    def add_children_subscriber(self, func):
        """Add a subscriber function for children event changes

        :param func: Function to register
        :type func: function

        The subscriber function will be called and passed two
        parameters, the node, and a list of the children before the
        event was triggered. A list of the current children can be
        found on :obj:`ZkNode.children`.

        .. warning::

            Subscriber functions must exist at compile-time to be
            registered, as weak references are used for subscription.
            This means functions declared within function scope cannot
            be subscribed.

        """
        self.children_change.connect(func)

    def add_data_subscriber(self, func):
        """Add a subscriber function for data value changes

        :param func: Function to register
        :type func: function

        The subscriber function will be called and passed two
        parameters, the node, and the prior value of the node. The
        current value can be found on :obj:`ZkNode.value`.

        .. warning::

            Subscriber functions must exist at compile-time to be
            registered, as weak references are used for subscription.
            This means functions declared within function scope cannot
            be subscribed.

        """
        self.data_change.connect(func)

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
        """Set the value with a new one

        :param value: The value of the node
        :type value: Any str'able object

        """
        with self._cv:
            self._zk.set(
                self._path, _save_value(value, use_json=self._use_json))

            # Now wait to see that it triggered our change event
            self._cv.wait()

    @property
    def children(self):
        """Returns the node's children

        If the Zookeeper session expired, it will be reconnected and
        the value reloaded.

        """
        if self._reload_children:
            self._load_children()
            self._reload_children = False
        return self._children

    @property
    def connected(self):
        """Indicate whether a connection to Zookeeper exists"""
        return self._zk.connected


class ZkNodeDict(UserDict.DictMixin):
    """Zookeeper Node Dict

    This object loads a shallow node tree from Zookeeper and
    represents it as a dict. Each dict name/value represents
    a node under the parent path, and updates in Zookeeper to
    remove/add nodes or change values are immediately represented
    in the :class:`ZkNodeDict` object.

    The full range of Python dict operations are available with
    the :class:`ZkNodeDict`.

    Example::

        from zktools.connection import ZkConnection
        from zktools.node import ZkNode

        conn = ZkConnection()
        nodedict = ZkNodeDict(conn, '/some/config/nodetree')

        # see the keys
        print nodedict.keys()

        # set a value
        nodedict['my_key'] = 23

        # delete a value
        del nodedict['my_key']


    """
    def __init__(self, connection, path, permission=ZOO_OPEN_ACL_UNSAFE):
        """Create a ZkNodeDict object

        :param connection: zookeeper connection object
        :type connection: ZkConnection instance
        :param path: Path to the Zookeeper node
        :type path: str
        :param permission: Node permission to use if the node is being
                           created.
        :type permission: dict

        """
        self._zk = connection
        self._path = path
        self._node_dict = {}
        self._cv = threading.Event()
        self._permission = permission

        # Update children nodes

        # Do our initial load of the main node
        self._node = ZkNode(self._zk, self._path)

        @self._zk.children(self._path)
        def child_watcher(children):
            old_set = set(self._node_dict.keys())
            new_set = set(children)
            for name in new_set - old_set:
                if name in self._node_dict:
                    continue
                self._node_dict[name] = ZkNode(self._zk,
                    '%s/%s' % (self._path, name),
                    permission=self._permission)
            for name in old_set - new_set:
                del self._node_dict[name]
            self._cv.set()

        # We hold a reference to our function to ensure its still
        # tracked since the decorator above uses a weak-ref
        self._child_watch = child_watcher

    def keys(self):
        """Return the current node keys"""
        return self._node_dict.keys()

    def __getitem__(self, name):
        """Retrieve the value from the underlying node"""
        if name not in self._node_dict:
            raise KeyError
        else:
            return self._node_dict[name].value

    def __setitem__(self, name, value):
        """Set an item in the node tree"""
        if not isinstance(name, str):
            raise Exception("Key names must be strings.")
        if name in self._node_dict:
            self._node_dict[name].value = value
        else:
            self._node_dict[name] = ZkNode(self._zk,
                '%s/%s' % (self._path, name), default=value,
                permission=self._permission)

    def __delitem__(self, name):
        """Delete an item in Zookeeper, and wait for the
        delete notification to remove it from the ZkNodeDict"""
        self._cv.clear()
        if name not in self._node_dict:
            raise KeyError
        else:
            self._zk.delete('%s/%s' % (self._path, name))
            # Wait for the delete to trigger
            self._cv.wait()
