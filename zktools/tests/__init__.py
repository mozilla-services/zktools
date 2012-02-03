import unittest

__all__ = ['TestBase']


connection = []


class TestBase(unittest.TestCase):
    @property
    def conn(self):
        from zc.zk import ZooKeeper
        if connection:
            conn = connection[0]
        else:
            conn = ZooKeeper()
            connection.append(conn)
        return conn
