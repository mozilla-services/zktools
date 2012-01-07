import unittest

__all__ = ['TestBase']


connection = []


class TestBase(unittest.TestCase):
    @property
    def conn(self):
        from zktools.connection import ZkConnection
        if not connection:
            connection.append(ZkConnection())
        return connection[0]
