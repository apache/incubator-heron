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
      epilog = 'For detailed documentation, go to http://heron.github.io', 
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
# Check whether the classpath provided is valid
################################################################################
def check_classpath(classpath):
  cpaths = classpath.split(':')
  if len(cpaths) == 1 and not cpaths[0]:
    return True
  for cp in cpaths:
    if not cp: 
      Log.error('Invalid class path: %s' % (classpath))
      return False
    elif cp.endswith('*'):
      if not os.path.isdir(os.path.dirname(cp)): 
        Log.error('Class path entry %s not a directory' % (cp))
        return False
      else:
        continue
    elif not os.path.isfile(cp):
      Log.error('Invalid class path entry: %s' % (cp))
      return False

  return True 

################################################################################
# Check validity of the parameters
################################################################################
def check_parameters(command_line_args):
  classpath = command_line_args['classpath']
  if classpath and not check_classpath(classpath):
    sys.exit(1)

  try:
    if command_line_args['verbose']: 
      opts.set_verbose()
  except:
    pass


################################################################################
# Extract all the common args for all commands
################################################################################
def extract_common_args(command, parser, cl_args):
  new_cl_args = dict()
  try:
    cluster_role_env = cl_args.pop('cluster/[role]/[env]')
    cluster_tuple = utils.parse_cluster_role_env(cluster_role_env)

    config_path = cl_args['config_path']

  except KeyError:
    # if some of the arguments are not found, print error and exit
    subparser = utils.get_subparser(parser, command)
    print(subparser.format_help())
    return dict()

  config_path = utils.get_heron_cluster_conf_dir(cluster_role_env, config_path);
  if not os.path.isdir(config_path):
    Log.error("Config path directory does not exist: %s" % config_path)
    return dict()

  new_cl_args['cluster'] = cluster_tuple[0]
  new_cl_args['role'] = cluster_tuple[1]
  new_cl_args['environ'] = cluster_tuple[2]
  new_cl_args['config_path'] = config_path

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

  # check various parameters and if valid set appropriate options
  check_parameters(command_line_args)

  # command to be execute
  command = command_line_args['subcommand']

  if command != 'help' and command != 'version':
    command_line_args = extract_common_args(command, parser, command_line_args) 

  start = time.time() 
  retcode = run(command, parser, command_line_args, unknown_args)
  end = time.time()

  if command != 'help':
    sys.stdout.flush()
    Log.info('Elapsed time: %.3fs.' % (end-start))

  return 0 if retcode == True else 1

if __name__ == "__main__":
  sys.exit(main())
