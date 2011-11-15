import unittest
import mock


class TestConnection(unittest.TestCase):
    def makeOne(self, *args, **kwargs):
        from zktools.connection import ZkConnection
        return ZkConnection(*args, **kwargs)

    def test_connect(self):
        zkc = self.makeOne()
        zkc.cv = mock.Mock()
        mock_zc = mock.Mock()
        mock_init = mock_zc.init
        mock_init.side_effect = async_connect

        mock_zc.SESSION_EVENT = 1
        mock_zc.CONNECTED_STATE = 3

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            zkc.connect()


def async_connect(host, func, timeout):
    """Emulates a successfull connect callback"""
    func(0, 1, 3, None)
    return 0
