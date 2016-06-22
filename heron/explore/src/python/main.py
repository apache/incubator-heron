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

#!/usr/bin/env python2.7

import argparse
import logging
import sys
import time

# command-line parsing
import heron.explore.src.python.args as parse
import heron.explore.src.python.help as help
import heron.explore.src.python.topology as topology
import heron.explore.src.python.logicalplan as logicalplan
import heron.explore.src.python.physicalplan as physicalplan
import heron.explore.src.python.show as show
from tornado.options import define

LOG = logging.getLogger(__name__)

# default parameter - url to connect to heron tracker
DEFAULT_TRACKER_URL = "http://localhost:8888"

class _HelpAction(argparse._HelpAction):
  def __call__(self, parser, namespace, values, option_string=None):
    parser.print_help()

    # retrieve subparsers from parser
    subparsers_actions = [
      action for action in parser._actions
      if isinstance(action, argparse._SubParsersAction)]

    # there will probably only be one subparser_action,
    # but better save than sorry
    for subparsers_action in subparsers_actions:
      # get all subparsers and print help
      for choice, subparser in subparsers_action.choices.items():
        print("Subparser '{}'".format(choice))
        print(subparser.format_help())
        return

class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  def _format_action(self, action):
    parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts

################################################################################
# Main parser
################################################################################
def create_parser():
  help_epilog = '''Getting more help:
  heron-explore help <command> Prints help and options for <command>\n
  For detailed documentation, go to http://heronstreaming.io'''

  parser = argparse.ArgumentParser(
      prog='heron-explore',
      epilog=help_epilog,
      formatter_class=SubcommandHelpFormatter,
      add_help=False)

  # option to specify tracker_url
  parser.add_argument(
    '--tracker_url',
    metavar='(tracker url; default: "' + DEFAULT_TRACKER_URL + '")',
    type=str, default=DEFAULT_TRACKER_URL)

  parser.add_argument(
    '--verbose',
    action='store_true')

  # sub-commands
  subparsers = parser.add_subparsers(
      title="Available commands",
      metavar='<command> <options>')

  # subparser for subcommands related to topology in general
  topology.create_parser(subparsers)

  # subparser for subcommands related to logical plan
  logicalplan.create_parser(subparsers)

  # subparser for subcommands related to physical plan
  physicalplan.create_parser(subparsers)

  # subparser for subcommands related to displaying info
  show.create_parser(subparsers)

  # subparser for help subcommand
  help.create_parser(subparsers)
  return parser

################################################################################
# Run the command
################################################################################
def run(command, *args):
  if command == 'topology': # remove
    return topology.run(command, *args)
  if command == 'components':
    return logicalplan.run(command, *args)
  if command == 'metrics':
    return physicalplan.run_metrics(command, *args)
  if command == 'show':
    return show.run(command, *args)
  if command == 'help':
    return help.run(command, *args)
  return 1

def configure_logging(level):
  log_format = "%(asctime)s-%(levelname)s:%(filename)s:%(lineno)s: %(message)s"
  date_format = '%d %b %Y %H:%M:%S'
  logging.basicConfig(format=log_format, datefmt=date_format, level=level)

################################################################################
# Run the command
################################################################################
def main():

  # create the argument parser
  parser = create_parser()

  # if no argument is provided, print help and exit
  if len(sys.argv[1:]) == 0:
    parser.print_help()
    return 0

  # insert the boolean values for some of the options
  sys.argv = parse.insert_bool_values(sys.argv)

  # parse the args
  args, unknown_args = parser.parse_known_args()
  command_line_args = vars(args)

  # set tracker_url variable globally
  define("tracker_url", command_line_args["tracker_url"])

  command = command_line_args['subcommand']

  if command_line_args['verbose']:
    LOG.setLevel(logging.DEBUG)
    configure_logging(logging.DEBUG)
  else:
    LOG.setLevel(logging.INFO)
    configure_logging(logging.INFO)

  if command != 'help':
    LOG.debug("Using tracker URL: %s", command_line_args["tracker_url"])

  # timing command execution
  start = time.time()
  ret = run(command, parser, command_line_args, unknown_args)
  end = time.time()

  if command != 'help':
    sys.stdout.flush()
    LOG.info('Elapsed time: %.3fs.' % (end-start))

  return 0 if ret else 1

if __name__ == "__main__":
  sys.exit(main())
