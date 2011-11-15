import unittest
import mock


class TestLocking(unittest.TestCase):
    def makeOne(self, *args, **kwargs):
        from zktools.locking import ZkLock
        return ZkLock(*args, **kwargs)

    def test_connect(self):
        mock_conn = mock.Mock()
        mock_conn.cv = mock.Mock()
        lock = self.makeOne(mock_conn, 'lock')

        mock_zc = mock.Mock()
        mock_zc.SESSION_EVENT = 1
        mock_zc.CONNECTED_STATE = 3

        results = [
            '/ZktoolsLocks',
            '/ZktoolsLocks/lock',
            '/ZktoolsLocks/lock/lock0001'
        ]
        results.reverse()

        def pop_arg(*args):
            return results.pop()
        mock_conn.create.side_effect = pop_arg
        mock_conn.get_children.return_value = ['lock0001']

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            result = lock.acquire()
            self.assertEqual(result, True)
            result = lock.release()
            self.assertEqual(result, True)
