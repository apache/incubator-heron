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

import os
import argparse
import heron.explore.src.python.help as help


help_epilog = '''Getting more help:
  heron-explore help <command> Prints help and options for <command>

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

def add_titles(parser):
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"  
  return parser

def insert_bool(param, command_args):
  index = 0 
  found = False
  for lelem in command_args:
    if lelem == '--' and not found:
      break
    if lelem == param:
      found = True
      break
    index = index + 1   

  if found:
    command_args.insert(index + 1, 'True')
  return command_args 

def insert_bool_values(command_line_args):
  args1 = insert_bool('--verbose', command_line_args)
  return args1

def req(req, label, subcommand):
  return subcommand if req and not label else "--" + subcommand

def add_verbose(parser):
  parser.add_argument(
      req(False, False, 'verbose'),
      metavar='(a boolean; default: "false")',
      default=False)
  return parser

def add_topology(parser, required=False, labelled=False):
  parser.add_argument(
      req(required, labelled, 'topology'),
      help='Name of the topology',
      metavar='TOPOLOGY', type=str)
  return parser

def add_cluster(parser, required=False, labelled=False):
  parser.add_argument(
    req(required, labelled, 'cluster'),
    help='Name of cluster',
    metavar='CLUSTER', type=str)
  return parser

def add_env(parser, required=False, labelled=False):
  parser.add_argument(
    req(required, labelled, 'env'),
    help='Environment',
    metavar='ENV', type=str)
  return parser

def add_role(parser, required=False, labelled=False):
  parser.add_argument(
    req(required, labelled, 'role'),
    help='Roles',
    metavar='ROLE', type=str)
  return parser

def add_container_id(parser, required=False, labelled=False):
  parser.add_argument(
    req(required, labelled, 'cid'),
    help='Container ID',
    type=int, metavar='CONTAINER_ID')
  return parser

def add_spout_name(parser, required=False, labelled=False):
  parser.add_argument(
    req(required, labelled, 'spout'),
    help='spout name',
    type=str, metavar='SPOUT_ID')
  return parser

def add_bolt_name(parser, required=False, labelled=False):
  parser.add_argument(
    req(required, labelled, 'bolt'),
    help='bolt name',
    type=str, metavar='BOLT_ID')
  return parser

def add_cluster_env_topo(parser):
  parser.add_argument(
    '[cluster]/[env]/[topology]',
    help='Cluster, environment, and topology name'
  )
  return parser

def add_spouts(parser):
  parser.add_argument(
    '--spout', help='display spout', action='store_true')
  return parser

def add_bolts(parser):
  parser.add_argument(
    '--bolt', help='display bolt', action='store_true')
  return parser
