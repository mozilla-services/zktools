from nose.tools import eq_

from zktools.tests import TestBase


class TestConnection(TestBase):
    def tearDown(self):
        try:
            self.conn.delete('/zktoolsTest')
            self.conn.close()
        except:  # nocover
            pass

    def test_connect(self):
        self.conn.connect()
        eq_(self.conn.connected, True)

    def test_create_nodes(self):
        from zktools.configuration import ZOO_OPEN_ACL_UNSAFE
        conn = self.conn
        conn.connect()
        conn.create('/zktoolsTest', '01', [ZOO_OPEN_ACL_UNSAFE], 0)
        val, _ = conn.get('/zktoolsTest')
        eq_(val, '01')
