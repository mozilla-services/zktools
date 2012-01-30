import unittest

__all__ = ['TestBase']


connection = []


class TestBase(unittest.TestCase):
    @property
    def conn(self):
        from zktools.connection import ZkConnection
        if connection:
            conn = connection[0]
        else:
            conn = ZkConnection()
            connection.append(conn)
        if not conn.connected:
            conn.connect()
        return conn
