#!/usr/bin/python2.7

import argparse
import utils

def create_parser(subparsers):
  parser = subparsers.add_parser(
      'help', 
      help='Prints help for commands', 
      add_help = False)

  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"

  parser.add_argument(
      'help-command',
      nargs = '?',
      default = 'help',
      help='Provide help for a command')

  parser.set_defaults(subcommand='help')
  return parser

def run(command, parser, args, unknown_args):
  # get the command for detailed help
  command_help = args['help-command']
  
  # if no command is provided, just print main help
  if command_help == 'help':
    parser.print_help()
    return True

  # get the subparser for the specific command
  subparser = utils.get_subparser(parser, command_help)
  if subparser:
    print(subparser.format_help())
    return False

  return True
