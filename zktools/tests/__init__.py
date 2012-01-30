import unittest

__all__ = ['TestBase']


connection = []


class TestBase(unittest.TestCase):
    @property
    def conn(self):
        from zktools.connection import ZkConnection
        if not connection:
            conn = ZkConnection()
            conn.connect()
            connection.append(conn)
        if not connection[0].connected:
            connection[0].connect()
        return connection[0]
