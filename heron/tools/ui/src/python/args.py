#!/usr/bin/env python
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

''' args.py '''
import argparse

import heron.tools.ui.src.python.consts as consts


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

    parser.exit()


class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  ''' SubcommandHelpFormatter '''

  def _format_action(self, action):
    # pylint: disable=bad-super-call
    parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts


def add_titles(parser):
  '''
  :param parser:
  :return:
  '''
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser


def add_arguments(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--tracker_url',
      metavar='(a url; path to tracker; default: "' + consts.DEFAULT_TRACKER_URL + '")',
      default=consts.DEFAULT_TRACKER_URL)

  parser.add_argument(
      '--address',
      metavar='(an string; address to listen; default: "' + consts.DEFAULT_ADDRESS + '")',
      default=consts.DEFAULT_ADDRESS)

  parser.add_argument(
      '--port',
      metavar='(an integer; port to listen; default: ' + str(consts.DEFAULT_PORT) + ')',
      type=int,
      default=consts.DEFAULT_PORT)

  parser.add_argument(
      '--base_url',
      metavar='(a string; the base url path if operating behind proxy; default: '
      + str(consts.DEFAULT_BASE_URL) + ')',
      default=consts.DEFAULT_BASE_URL)

  return parser


def create_parsers():
  '''
  :return:
  '''
  parser = argparse.ArgumentParser(
      epilog='For detailed documentation, go to http://heronstreaming.io',
      usage="%(prog)s [options] [help]",
      add_help=False)

  parser = add_titles(parser)
  parser = add_arguments(parser)

  # create the child parser for subcommand
  child_parser = argparse.ArgumentParser(
      parents=[parser],
      formatter_class=SubcommandHelpFormatter,
      add_help=False)

  # subparser for each command
  subparsers = child_parser.add_subparsers(
      title="Available commands")

  help_parser = subparsers.add_parser(
      'help',
      help='Prints help',
      add_help=False)

  version_parser = subparsers.add_parser(
      'version',
      help='Prints version',
      add_help=True)

  help_parser.set_defaults(help=True)
  version_parser.set_defaults(version=True)
  return (parser, child_parser)
