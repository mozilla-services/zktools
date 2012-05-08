# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Utility functions for Zookeeper"""
import uuid
from functools import wraps
from threading import Thread

import zookeeper


def safe_call(zk, func, *args, **kwargs):
    """Safely call a function while handling connection loss

    .. note::

        This function merely retries the query until an active
        Zookeeper session is available that returns without throwing
        a ConnectionLoss Exception. When creating a node, this could
        result in a NodeAlreadyExists exception because the create
        ran twice.

    """
    while 1:
        try:
            return getattr(zk, func)(*args, **kwargs)
        except (zookeeper.ClosingException,
                zookeeper.ConnectionLossException,
                zookeeper.OperationTimeoutException):
            zk.connected.wait()


def safe_create_ephemeral_sequence(zk, name, data, acl):
    """Safely creates an ephemeral sequence node using a UUID prefix

    This function properly handles zookeeper ConnectionLoss exceptions
    and determines after waiting for reconnection whether it was created
    successfully.

    :param zk: Zookeeper instance
    :param name: Name of the node, it will be prefixed by the UUID
    :param data: Data to set on the node
    :param acl: ACL to set for the node
    :returns: Name of the created node

    The name will be split so that its prefixed by the UUID and
    suffixed by the sequence.

    Example:

    .. code-block:: pycon

        >>> safe_create_ephemeral_sequence(zk, '/path/to/node/myname',
                                           [ZOO_OPEN_ACL_UNSAFE])
        '/path/to/node/dfad3fa294d745e499d883b0a38bbc93-myname-0001'

    """
    prefix = uuid.uuid4().hex + '-'
    path, node_name = name.rsplit('/', 1)
    node_name = '/'.join([path, prefix + node_name + '-'])

    while 1:
        try:
            return zk.create(node_name, data, acl,
                             zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
        except (zookeeper.ClosingException,
                zookeeper.ConnectionLossException,
                zookeeper.OperationTimeoutException):
            # Check children to see if the node was created
            children = safe_call(zk, 'get_children', path)
            created = [x for x in children if x.startswith(prefix)]
            if created:
                return '/'.join([path, created[0]])
            # We've verified the create failed, retry
            continue


def threaded(func):
    """Decorator to run a function in a separate thread

    :param func: Function to run in the other thread
    :returns: :class:`Thread` reference

    Example::

        @threaded
        def task1():
            do_something

        @threaded
        def task2():
            do_something_too

        t1 = task1()
        t2 = task2()
        ...
        t1.join()
        t2.join()

    """
    @wraps(func)
    def threaded_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl
    return threaded_func
