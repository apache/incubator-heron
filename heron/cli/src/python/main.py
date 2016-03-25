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
import heron.cli.src.python.classpath as classpath
import heron.cli.src.python.deactivate as deactivate
import heron.cli.src.python.kill as kill
import heron.cli.src.python.restart as restart
import heron.cli.src.python.submit as submit
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
      epilog = 'For detailed documentation, go to http://go/heron', 
      formatter_class=SubcommandHelpFormatter,
      add_help = False)

  subparsers = parser.add_subparsers(
      title = "Available commands", 
      metavar = '<command> <options>')

  activate.create_parser(subparsers)
  classpath.create_parser(subparsers)
  deactivate.create_parser(subparsers)
  help.create_parser(subparsers)
  kill.create_parser(subparsers)
  restart.create_parser(subparsers)
  submit.create_parser(subparsers)
  version.create_parser(subparsers)

  return parser

################################################################################
# Main execute 
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
  namespace = vars(args)

  try:
    if namespace['verbose']: 
      opts.set_verbose()
    if namespace['trace_execution']:
      opts.set_trace_execution()
  except:
    pass

  # register cleanup function during exit
  atexit.register(cleanup)

  # command to be execute
  command = namespace['subcommand']

  start = time.time() 
  retcode = run(command, parser, namespace, unknown_args)
  end = time.time()

  if command != 'help':
    sys.stdout.flush()
    Log.info('Elapsed time: %.3fs.' % (end-start))

  return 0 if retcode == True else 1

if __name__ == "__main__":
  sys.exit(main())
