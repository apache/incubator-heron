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

#!/usr/bin/python2.7

import argparse
import atexit
import base64
import contextlib
import glob
import logging
import logging.handlers
import os
import shutil
import sys
import subprocess
import tarfile
import tempfile
import time

from heron.common.src.python.color import Log

import heron.cli.src.python.help as help
import heron.cli.src.python.args as parse
import heron.cli.src.python.opts as opts
import heron.cli.src.python.activate as activate
import heron.cli.src.python.deactivate as deactivate
import heron.cli.src.python.kill as kill
import heron.cli.src.python.restart as restart
import heron.cli.src.python.submit as submit
import heron.cli.src.python.utils as utils
import heron.cli.src.python.version as version

help_epilog = '''Getting more help: 
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''

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
  parser = argparse.ArgumentParser(
      prog = 'heron',
      epilog = help_epilog,
      formatter_class=SubcommandHelpFormatter,
      add_help = False)

  subparsers = parser.add_subparsers(
      title = "Available commands", 
      metavar = '<command> <options>')

  activate.create_parser(subparsers)
  deactivate.create_parser(subparsers)
  help.create_parser(subparsers)
  kill.create_parser(subparsers)
  restart.create_parser(subparsers)
  submit.create_parser(subparsers)
  version.create_parser(subparsers)

  return parser

################################################################################
# Run the command
################################################################################
def run(command, parser, command_args, unknown_args):
  if command == 'activate':
    return activate.run(command, parser, command_args, unknown_args)

  elif command == 'deactivate':
    return deactivate.run(command, parser, command_args, unknown_args)

  elif command == 'kill':
    return kill.run(command, parser, command_args, unknown_args)

  elif command == 'restart':
    return restart.run(command, parser, command_args, unknown_args)
  
  elif command == 'submit':
    return submit.run(command, parser, command_args, unknown_args)

  elif command == 'help':
    return help.run(command, parser, command_args, unknown_args)

  elif command == 'version':
    return version.run(command, parser, command_args, unknown_args)

  return 1

def cleanup():
  pass

################################################################################
# Check whether the environment variables are set
################################################################################
def check_environment():
  if not utils.check_java_home_set():
    sys.exit(1)

################################################################################
# Extract all the common args for all commands
################################################################################
def extract_common_args(command, parser, cl_args):
  try:
    cluster_role_env = cl_args.pop('cluster/[role]/[env]')
    config_path = cl_args['config_path']
  except KeyError:
    # if some of the arguments are not found, print error and exit
    subparser = utils.get_subparser(parser, command)
    print(subparser.format_help())
    return dict()

  cluster = utils.get_heron_cluster(cluster_role_env)
  config_path = utils.get_heron_cluster_conf_dir(cluster, config_path)
  if not os.path.isdir(config_path):
    Log.error("Config path directory does not exist: %s" % config_path)
    return dict()

  new_cl_args = dict()
  try:
    cluster_tuple = utils.parse_cluster_role_env(cluster_role_env, config_path)
    new_cl_args['cluster'] = cluster_tuple[0]
    new_cl_args['role'] = cluster_tuple[1]
    new_cl_args['environ'] = cluster_tuple[2]
    new_cl_args['config_path'] = config_path
  except Exception as e:
    Log.error("Argument cluster/[role]/[env] is not correct: %s" % str(e))
    return dict()

  cl_args.update(new_cl_args)
  return cl_args

################################################################################
# Run the command
################################################################################
def main():

  # verify if the environment variables are correctly set
  check_environment()

  # register cleanup function during exit
  atexit.register(cleanup)

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

  try:
    if command_line_args['verbose']: 
      opts.set_verbose()
    if command_line_args['trace_execution']:
      opts.set_trace_execution()
  except:
    pass

  # command to be execute
  command = command_line_args['subcommand']

  if command != 'help' and command != 'version':
    command_line_args = extract_common_args(command, parser, command_line_args) 

  # bail out if args are empty
  if not command_line_args:
    return 1

  start = time.time() 
  retcode = run(command, parser, command_line_args, unknown_args)
  end = time.time()

  if command != 'help':
    sys.stdout.flush()
    Log.info('Elapsed time: %.3fs.' % (end-start))

  return 0 if retcode == True else 1

if __name__ == "__main__":
  sys.exit(main())
