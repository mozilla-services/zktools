import unittest

from nose.tools import eq_


class TestConnection(unittest.TestCase):
    def makeOne(self, *args, **kwargs):
        from zktools.connection import ZkConnection
        return ZkConnection(*args, **kwargs)

    def tearDown(self):
        try:
            conn = self.makeOne()
            conn.delete('/zktoolsTest')
        except:  # nocover
            pass

    def test_connect(self):
        zkc = self.makeOne()
        zkc.connect()
        eq_(zkc.connected, True)

    def test_create_nodes(self):
        from zktools.configuration import ZOO_OPEN_ACL_UNSAFE
        conn = self.makeOne()
        conn.connect()
        conn.create('/zktoolsTest', '01', [ZOO_OPEN_ACL_UNSAFE], 0)
        val, _ = conn.get('/zktoolsTest')
        eq_(val, '01')
