import datetime
import decimal
import time

from nose.tools import eq_


from zktools.tests import TestBase


class TestNode(TestBase):
    def makeOne(self, *args, **kwargs):
        from zktools.node import ZkNode
        return ZkNode(self.conn, *args, **kwargs)

    def setUp(self):
        n = self.makeOne('/zkTestNode')
        n._zk.delete('/zkTestNode')

    def testSetDefaultValue(self):
        n = self.makeOne('/zkTestNode', 234)
        eq_(n.value, decimal.Decimal('234'))

    def testUpdateValue(self):
        n1 = self.makeOne('/zkTestNode')
        n2 = self.makeOne('/zkTestNode')

        eq_(n1.value, n2.value)

        n1.value = 942

        # It can take a fraction of a second on some machines on occasion
        # for the other value to update
        time.sleep(0.1)
        eq_(n1.value, n2.value)

    def testJsonValue(self):
        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = dict(alpha=203)
        eq_(n1.value['alpha'], 203)
        n2 = self.makeOne('/zkTestNode', use_json=True)
        eq_(n2.value, n1.value)

    def testBadJson(self):
        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = '{[sasdfasdfsd}'
        n2 = self.makeOne('/zkTestNode', use_json=True)
        eq_(n2.value, n1.value)

    def testNormalString(self):
        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = 'fred'
        n2 = self.makeOne('/zkTestNode', use_json=True)
        eq_(n2.value, n1.value)

    def testFloat(self):
        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = 492.23
        n2 = self.makeOne('/zkTestNode', use_json=True)
        eq_(n2.value, n1.value)

    def testDates(self):
        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = datetime.datetime.today()
        n2 = self.makeOne('/zkTestNode', use_json=True)
        eq_(n2.value, n1.value)

        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = datetime.date.today()
        n2 = self.makeOne('/zkTestNode', use_json=True)
        eq_(n2.value, n1.value)

    def testReload(self):
        n1 = self.makeOne('/zkTestNode', use_json=True)
        n1.value = now = datetime.datetime.today()
        n1._reload = True
        eq_(n1.value, now)
