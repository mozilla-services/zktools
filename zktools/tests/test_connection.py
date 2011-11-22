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

    def test_proxy_command(self):
        zkc = self.makeOne()
        zkc.cv = mock.Mock()
        mock_zc = mock.Mock()
        mock_init = mock_zc.init
        mock_init.side_effect = async_connect

        mock_zc.SESSION_EVENT = 1
        mock_zc.CONNECTED_STATE = 3
        mock_zc.get.return_value = 'works'

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            zkc.connect()
            self.assertEqual(zkc.get('/some/key'), 'works')

    def test_proxy_command_reconnect(self):
        zkc = self.makeOne()
        zkc.cv = mock.Mock()
        mock_zc = mock.Mock()
        mock_init = mock_zc.init
        mock_init.side_effect = async_connect

        mock_zc.SESSION_EVENT = 1
        mock_zc.CONNECTED_STATE = 3
        import zookeeper
        mock_zc.ConnectionLossException = zookeeper.ConnectionLossException
        mock_zc.get.side_effect = sequence(
            'works',
            zookeeper.ConnectionLossException(),
            'stillworks',
        )

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            zkc.connect()
            self.assertEqual(zkc.get('/some/key'), 'works')
            self.assertEqual(zkc.get('/some/key'), 'stillworks')

    def test_connect_automatically(self):
        mock_zc = mock.Mock()
        mock_init = mock_zc.init
        mock_init.side_effect = async_connect

        mock_zc.SESSION_EVENT = 1
        mock_zc.CONNECTED_STATE = 3
        mock_zc.get.return_value = 'works'

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            zkc = self.makeOne()
            zkc.cv = mock.Mock()
            self.assertEqual(zkc.get('/some/key'), 'works')


def sequence(*args):
    orig_values = args
    values = list(reversed(args))

    def return_value(*args):  # pragma: nocover
        try:
            val = values.pop()
            if isinstance(val, Exception):
                raise val
            else:
                return val
        except IndexError:
            print orig_values
            raise
    return return_value


def async_connect(host, func, timeout):
    """Emulates a successfull connect callback"""
    func(0, 1, 3, None)
    return 0
