#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

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

''' help.py '''
from heron.common.src.python.utils.log import Log
from heron.tools.cli.src.python.result import SimpleResult, Status
from heron.tools.common.src.python.utils import config

def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'help',
      help='Prints help for commands',
      add_help=True)

  # pylint: disable=protected-access
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"

  parser.add_argument(
      'help-command',
      nargs='?',
      default='help',
      help='Provide help for a command')

  parser.set_defaults(subcommand='help')
  return parser


# pylint: disable=unused-argument,superfluous-parens
def run(command, parser, args, unknown_args):
  '''
  :param command:
  :param parser:
  :param args:
  :param unknown_args:
  :return:
  '''
  # get the command for detailed help
  command_help = args['help-command']

  # if no command is provided, just print main help
  if command_help == 'help':
    parser.print_help()
    return SimpleResult(Status.Ok)

  # get the subparser for the specific command
  subparser = config.get_subparser(parser, command_help)
  if subparser:
    print(subparser.format_help())
    return SimpleResult(Status.Ok)
  Log.error("Unknown subcommand \'%s\'", command_help)
  return SimpleResult(Status.InvocationError)
