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
import argparse as argparser
import logging
import os
import sys
import unittest2 as unittest
import heron.tools.common.src.python.utils.heronparser as hr_argparser
import heron.tools.common.src.python.utils.config as config
import heron.tools.cli.src.python.activate as activate
import heron.tools.cli.src.python.submit as submit

logging.basicConfig(level=logging.INFO)


#pylint: disable=missing-docstring, no-self-use
help_epilog = '''Getting more help:
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''

class HeronParserTest(unittest.TestCase):
  logging.basicConfig(level=logging.WARNING)

  def setUp(self):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    self.testrcfile = dir_path + "/heronrc.test"
    self.testrc_submit_file = dir_path + "/heronrc.test.submit"

  def test_parser_commandline_norc(self):
    sys.argv = []

    parser = argparser.ArgumentParser(
        prog='heron',
        epilog=help_epilog,
        formatter_class=config.SubcommandHelpFormatter,
        fromfile_prefix_chars='@',
        add_help=False)

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')
    submit.create_parser(subparsers)
    args, _ = parser.parse_known_args(["submit", "local", "~/.heron/examples/heron-examples.jar",
                                       "com.twitter.heron.examples.ExclamationTopology",
                                       "ExclamationTopology"])

    namespace = vars(args)
    self.assertEqual('submit', namespace['subcommand'])
    self.assertEqual('local', namespace['cluster/[role]/[env]'])
    self.assertEqual('~/.heron/examples/heron-examples.jar', namespace['topology-file-name'])
    self.assertEqual('com.twitter.heron.examples.ExclamationTopology', namespace['topology-class-name'])


  def test_parser_commandline_positional_withrc(self):
    sys.argv = []

    parser = hr_argparser.HeronArgumentParser(
        prog='heron',
        epilog=help_epilog,
        formatter_class=config.SubcommandHelpFormatter,
        fromfile_prefix_chars='@',
        add_help=False,
        rcfile=self.testrcfile,
        rccommand="submit",
        rcclusterrole="local")

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')
    submit.create_parser(subparsers)
    args, _ = parser.parse_known_args(["submit", "local", "~/.heron/examples/heron-examples.jar",
                                       "com.twitter.heron.examples.ExclamationTopology",
                                       "ExclamationTopology"])
    namespace = vars(args)
    self.assertEqual('True', namespace['verbose'])
    hr_argparser.HeronArgumentParser.clear()

  def test_parser_commandline_positional_error(self):
    sys.argv = []

    parser = hr_argparser.HeronArgumentParser(
        prog='heron',
        epilog=help_epilog,
        formatter_class=config.SubcommandHelpFormatter,
        fromfile_prefix_chars='@',
        add_help=False,
        rcfile=self.testrcfile,
        rccommand="submit",
        rcclusterrole="ilocal")

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')
    submit.create_parser(subparsers)
    try:
      _, _ = parser.parse_known_args(["submit", "ilocal"])
    except ValueError:
      pass
    else:
      self.fail('ValueError expected for test_parser_commandline_positional_error')
    hr_argparser.HeronArgumentParser.clear()


  def test_parser_commandline(self):
    sys.argv = []

    parser = hr_argparser.HeronArgumentParser(
        prog='heron',
        epilog=help_epilog,
        formatter_class=config.SubcommandHelpFormatter,
        fromfile_prefix_chars='@',
        add_help=False,
        rcfile=self.testrcfile,
        rccommand="activate",
        rcclusterrole="devcluster/ads/PROD")

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')
    activate.create_parser(subparsers)
    args, _ = parser.parse_known_args(["activate", "devcluster/ads/PROD",
                                       "12313", "--config-property", "a=b"])
    namespace = vars(args)
    self.assertEqual(['a=b', 'e=f', 'ooo=ppp', 'hi=wee', 'foo=bar'], args.config_property)
    hr_argparser.HeronArgumentParser.clear()

  def test_parser_rolecmdspecific(self):

    parser = hr_argparser.HeronArgumentParser(
        prog='heron',
        epilog=help_epilog,
        formatter_class=config.SubcommandHelpFormatter,
        fromfile_prefix_chars='@',
        add_help=False,
        rcfile=self.testrcfile,
        rccommand="activate",
        rcclusterrole="devcluster/ads/PROD")

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')
    activate.create_parser(subparsers)
    args, _ = parser.parse_known_args(["activate", "devcluster/ads/PROD",
                                       "12313"])
    self.assertEqual(['e=f', 'ooo=ppp', 'hi=wee', 'foo=bar'], args.config_property)
    hr_argparser.HeronArgumentParser.clear()

  def test_parser_norcfile(self):
    sys.argv = []

    parser = hr_argparser.HeronArgumentParser(
        prog='heron',
        epilog=help_epilog,
        formatter_class=config.SubcommandHelpFormatter,
        fromfile_prefix_chars='@',
        add_help=False,
        rcfile='INVALID',
        rccommand="activate",
        rcclusterrole="devcluster/ads/PROD")

    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar='<command> <options>')
    activate.create_parser(subparsers)
    args, _ = parser.parse_known_args(["activate", "devcluster/ads/PROD",
                                       "12313", "--config-property", "a=b"])

    self.assertEqual(['a=b'], args.config_property)
    hr_argparser.HeronArgumentParser.clear()

  def tearDown(self):
    hr_argparser.HeronArgumentParser.clear()
