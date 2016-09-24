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
''' help.py '''
from heron.common.src.python.utils.log import Log
import heron.tools.common.src.python.utils.config as config


# pylint: disable=protected-access
def create_parser(subparsers):
  """ create parser """
  parser = subparsers.add_parser(
      'help',
      help='Display help',
      add_help=False)

  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"

  parser.add_argument(
      'help-command',
      nargs='?',
      default='help',
      help='provide help for a command')

  parser.set_defaults(subcommand='help')
  return parser


# pylint: disable=unused-argument
def run(command, parser, args, unknown_args):
  """ run command """
  # get the command for detailed help
  command_help = args['help-command']

  # if no command is provided, just print main help
  if command_help == 'help':
    parser.print_help()
    return True

  # get the subparser for the specific command
  subparser = config.get_subparser(parser, command_help)
  if subparser:
    print subparser.format_help()
    return True
  else:
    Log.error("Unknown subcommand \'%s\'" % command_help)
    return False
