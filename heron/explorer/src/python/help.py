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

import logging
import utils
from heron.common.src.python.color import Log

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
    return True
  else:
    Log.error("Unknown subcommand \'%s\'" % command_help)
    return False
