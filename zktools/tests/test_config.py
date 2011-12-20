import unittest
import mock

from nose.tools import raises

import time
import datetime


class TestLocking(unittest.TestCase):
    def makeOne(self, *args, **kwargs):
        from zktools.configuration import ZkNode
        return ZkNode(*args, **kwargs)

    def test_load_value_coercion(self):
        mock_conn = mock.Mock()
        mock_conn.cv = mock.Mock()
        mock_zc = mock.Mock()
        mock_conn.get.return_value = ('124', {})

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            node = self.makeOne(mock_conn, '/somenode', load=True)
            self.assertEqual(node.value, 124)
            mock_conn.get.return_value = ('a value', {})
            node.load()
            self.assertEqual(node.value, 'a value')

    def test_save_value_coercion(self):
        mock_conn = mock.Mock()
        mock_conn.cv = mock.Mock()
        mock_zc = mock.Mock()
        mock_conn.get.return_value = ('124', {})

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            node = self.makeOne(mock_conn, '/somenode')
            val = time.time()
            node.create(val)
            ccal = mock_conn.method_calls[0]
            self.assertEqual(ccal[0], 'create')
            self.assertEqual(ccal[1][1], repr(val))
            d = datetime.datetime.utcnow()
            node.create(d)
            ccal = mock_conn.method_calls[-2]
            self.assertEqual(ccal[1][1], d.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
            d = datetime.date.today()
            node.create(d)
            ccal = mock_conn.method_calls[-2]
            self.assertEqual(ccal[1][1], d.strftime('%Y-%m-%d'))
            node.create('fred')
            ccal = mock_conn.method_calls[-2]
            self.assertEqual(ccal[1][1], 'fred')

    def test_set_exception(self):
        mock_conn = mock.Mock()
        mock_conn.cv = mock.Mock()
        node = self.makeOne(mock_conn, '/somenode')
        node.deleted = True

        @raises(Exception)
        def testit():
            node.set('hi')
        testit()

    def test_set(self):
        mock_conn = mock.Mock()
        mock_conn.cv = mock.Mock()
        mock_zc = mock.Mock()
        mock_conn.get.return_value = ('124', {})

        with mock.patch('zktools.connection.zookeeper', mock_zc):
            node = self.makeOne(mock_conn, '/somenode')
            node.set('fred')
            ccal = mock_conn.method_calls[0]
            self.assertEqual(ccal[1][1], 'fred')


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
