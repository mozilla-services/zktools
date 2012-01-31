.. _node_module:

:mod:`zktools.node`
===================

.. automodule:: zktools.node

Node Class
----------

.. autoclass:: ZkNode

    .. autoattribute:: last_modified

        The last time a :class:`ZkNode` has been modified either by
        the user or due to a Zookeeper update, recorded as seconds since
        epoch.

    :members: __init__, add_children_subscriber, add_data_subscriber, children, value, connected

.. autoclass:: ZkNodeDict
    :members: __init__
    :inherited-members:
