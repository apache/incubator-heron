#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


# !/usr/bin/env python2.7
''' main.py '''
import argparse
import os
import shutil
import sys
import traceback

import heron.common.src.python.utils.log as log
import heron.tools.common.src.python.utils.config as config
import heron.tools.cli.src.python.result as result
import heron.tools.admin.src.python.standalone as standalone

Log = log.Log

HELP_EPILOG = '''Getting more help:
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''

# pylint: disable=protected-access,superfluous-parens
class _HelpAction(argparse._HelpAction):
  def __call__(self, parser, namespace, values, option_string=None):
    parser.print_help()

    # retrieve subparsers from parser
    subparsers_actions = [
        action for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    ]

    # there will probably only be one subparser_action,
    # but better save than sorry
    for subparsers_action in subparsers_actions:
      # get all subparsers and print help
      for choice, subparser in list(subparsers_action.choices.items()):
        print("Subparser '{}'".format(choice))
        print(subparser.format_help())
        return

################################################################################
def get_command_handlers():
  '''
  Create a map of command names and handlers
  '''
  return {
      'standalone': standalone,
  }

################################################################################
def create_parser(command_handlers):
  '''
  Main parser
  :return:
  '''
  parser = argparse.ArgumentParser(
      prog='heron',
      epilog=HELP_EPILOG,
      formatter_class=config.SubcommandHelpFormatter,
      add_help=True)

  subparsers = parser.add_subparsers(
      title="Available commands",
      metavar='<command> <options>')

  command_list = sorted(command_handlers.items())
  for command in command_list:
    command[1].create_parser(subparsers)

  return parser

################################################################################
def run(handlers, command, parser, command_args, unknown_args):
  '''
  Run the command
  :param command:
  :param parser:
  :param command_args:
  :param unknown_args:
  :return:
  '''

  if command in handlers:
    return handlers[command].run(command, parser, command_args, unknown_args)
  else:
    err_context = 'Unknown subcommand: %s' % command
    return result.SimpleResult(result.Status.InvocationError, err_context)

def cleanup(files):
  '''
  :param files:
  :return:
  '''
  for cur_file in files:
    if os.path.isdir(cur_file):
      shutil.rmtree(cur_file)
    else:
      shutil.rmtree(os.path.dirname(cur_file))

################################################################################
def check_environment():
  '''
  Check whether the environment variables are set
  :return:
  '''

  if not config.check_release_file_exists():
    sys.exit(1)

################################################################################
def execute(handlers):
  '''
  Run the command
  :return:
  '''
  # verify if the environment variables are correctly set
  check_environment()

  # create the argument parser
  parser = create_parser(handlers)

  # if no argument is provided, print help and exit
  if len(sys.argv[1:]) == 0:
    parser.print_help()
    return 0

  # insert the boolean values for some of the options
  sys.argv = config.insert_bool_values(sys.argv)

  try:
    # parse the args
    args, unknown_args = parser.parse_known_args()
  except ValueError as ex:
    Log.error("Error while parsing arguments: %s", str(ex))
    Log.debug(traceback.format_exc())
    sys.exit(1)

  command_line_args = vars(args)

  # set log level
  log.set_logging_level(command_line_args)
  Log.debug("Input Command Line Args: %s", command_line_args)

  # command to be execute
  command = command_line_args['subcommand']

  # print the input parameters, if verbose is enabled
  Log.debug("Processed Command Line Args: %s", command_line_args)

  results = run(handlers, command, parser, command_line_args, unknown_args)

  return 0 if result.is_successful(results) else 1

def main():
  # Create a map of supported commands and handlers
  command_handlers = get_command_handlers()
  # Execute
  return execute(command_handlers)


if __name__ == "__main__":
  sys.exit(main())
