:mod:`zktools.connection` -- Zookeeper Connection Class
=======================================================

.. automodule:: zktools.connection

Implements a Zookeeper connection class that also proxies functional
calls to zookeeper functions and reconnects when necessary.

Module Contents
---------------

.. autoclass:: ZkConnection
    :members: __init__, connect, __getattr__
