import unittest2 as unittest
import heron.cli.src.python.opts as opts

class OptsTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_one_opt(self):
    opts.clear_config()
    opts.set_config('XX', 'xx')
    self.assertEqual('xx', opts.get_config('XX'))
    
  def test_two_opts(self):
    opts.clear_config()
    opts.set_config('XX', 'xx')
    opts.set_config('YY', 'yy')
    self.assertEqual('xx', opts.get_config('XX'))
    self.assertEqual('yy', opts.get_config('YY'))

  def test_non_exist_key(self):
    opts.clear_config()
    opts.set_config('XX', 'xx')
    self.assertEqual(None, opts.get_config('YY')) 

  def test_many_opts(self):
    opts.clear_config()
    for k in range(1, 100): 
      key = "key-%d" % (k)
      value = "value-%d" % (k)
      opts.set_config(key, value)

    for k in range(1, 100):
      key = "key-%d" % (k)
      value = "value-%d" % (k)
      self.assertEqual(value, opts.get_config(key))

  def test_clear_opts(self):
    opts.clear_config()
    opts.set_config('YY', 'yy')
    self.assertEqual('yy', opts.get_config('YY'))
    opts.clear_config()
    self.assertEqual(None, opts.get_config('YY'))
    
  def tearDown(self):
    opts.clear_config()

