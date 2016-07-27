# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
''' opts_unittest.py '''
import unittest2 as unittest
import heron.cli.src.python.activate as activate
import heron.common.src.python.argparser as argparser
import heron.common.src.python.utils as utils
import heron.cli.src.python.opts as opts
import sys

#pylint: disable=missing-docstring, no-self-use
help_epilog = '''Getting more help:
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''

class HeronRCTest(unittest.TestCase):
  def setUp(self):
    pass


  def test_parser_commandline(self):
    sys.argv=[]
    #print sys.argv


    parser = argparser.HeronRCArgumentParser(
      prog = 'heron',
      epilog = help_epilog,
      formatter_class=utils.SubcommandHelpFormatter,
      fromfile_prefix_chars='@',
      add_help = False,
      rcfile = "./.heronrc",
      rccommand = "activate",
      rcclusterrole="devcluster/ads/PROD")

    subparsers = parser.add_subparsers(
      title = "Available commands", 
      metavar = '<command> <options>')
    activate.create_parser(subparsers)
    args, unknown_args = parser.parse_known_args(["activate", "devcluster/ads/PROD", "12313", "--config-property", "this-is-it"])
    #print args.config_property[0]


    #print parser, args.config_property[0]

    self.assertEqual('this-is-it', args.config_property[0])

  def test_parser_rolecmdspecific(self):
    #sys.argv=["heron-cli", "activate", "devcluster/ads/PROD", "12313"]
    #print sys.argv
    parser = argparser.HeronRCArgumentParser(
      prog = 'heron',
      epilog = help_epilog,
      formatter_class=utils.SubcommandHelpFormatter,
      fromfile_prefix_chars='@',
      add_help = False,
      rcfile = "./.heronrc",
      rccommand = "activate",
      rcclusterrole="devcluster/ads/PROD")

    subparsers = parser.add_subparsers(
      title = "Available commands", 
      metavar = '<command> <options>')
    activate.create_parser(subparsers)
    print "\n"
    #args, unknown_args = parser.parse_known_args(["activate", "devcluster/ads/PROD", "12313"])
    args, unknown_args = parser.parse_known_args()
    print args
    self.assertEqual('test-cmd-activate-role', getattr(args,"config_property"))

  def tearDown(self):
    opts.clear_config()

def setup():
      parser = argparser.HeronRCArgumentParser(
      prog = 'heron',
      epilog = help_epilog,
      formatter_class=utils.SubcommandHelpFormatter,
      fromfile_prefix_chars='@',
      add_help = False,
      rcfile = "./.heronrc",
      rccommand = "activate",
      rcclusterrole="devcluster/ads/PROD")

      subparsers = parser.add_subparsers(
        title = "Available commands", 
        metavar = '<command> <options>')
      activate.create_parser(subparsers)
      args, unknown_args = parser.parse_known_args(["activate", "devcluster/ads/PROD", "12313", "--config-property", "this-is-it"])
      print args, unknown_args


if __name__ == '__main__':
    #suite = unittest.TestLoader().loadTestsFromTestCase(HeronRCTest)
    #unittest.TextTestRunner(verbosity=2).run(suite)

    #setup()
    suite = unittest.TestSuite()
    suite.addTest(HeronRCTest('test_parser_commandline'))
    unittest.TextTestRunner(verbosity=2).run(suite)
