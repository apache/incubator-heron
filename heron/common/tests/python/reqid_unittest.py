import unittest2 as unittest
from mock import call, patch, Mock
from heron.common.src.python.network.reqid import ReqId

class ReqIdUnitTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_generate(self):
    prev = 0
    for i in range(10):
      cur = ReqId.generate()
      self.assertEqual(len(str(cur)), 32)
      self.assertNotEqual(cur, prev)
      prev = cur