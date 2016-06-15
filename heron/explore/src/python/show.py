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
import tornado.gen
import tornado.ioloop
from heron.ui.src.python.handlers.access import heron as API
import heron.explore.src.python.args as args
from heron.explore.src.python.physicalplan import get_topology_info
import heron.explore.src.python.help as help
from tabulate import tabulate
import json
import utils

LOG = logging.getLogger(__name__)

# subsubparsers for roles and env are currently not supported
# because of design of Heron tracker API
# Tracker API does not have the concepts of roles
# see ``getTopologyInfo`` in ``heron/tracer/src/python/tracker.py``
# this function simply returns the first topology it finds that
# matches cluster/env/topology_name. It is possible that two different
# users may submit topologies of same names
# Bill: might not that important in real production since it rarely happens
def create_parser(subparsers):
  show_parser = subparsers.add_parser(
    'show',
    help='Show info',
    usage="%(prog)s [options]",
    add_help=False)

  show_parser.set_defaults(subcommand='show')

  # add subsubparsers: cluster, roles, and env
  subsubparsers = show_parser.add_subparsers(
    title="Available commands",
    metavar='<command> <options>')

  cparser = subsubparsers.add_parser(
    'cluster',
    help='Show cluster info',
    usage="%(prog)s [options]",# TODO: usage?
    add_help=False)
  cparser.set_defaults(subsubcommand='cluster')
  args.add_cluster(cparser, required=True)

  '''
  rparser = subsubparsers.add_parser(
    'roles',
    help='Show topologies submitted by selected role under certain cluster by specified roles',
    usage="%(prog)s [options]",# TODO: usage?
    add_help=False)
  rparser.set_defaults(subsubcommand='roles')
  args.add_role(rparser, required=True)
  args.add_cluster(rparser, required=True, labelled=True)
  '''

  eparser = subsubparsers.add_parser(
    'env',
    help='Show all topologies running submitted by certain people and under certain topologies\
          by specified roles',
    usage="%(prog)s [options]",# TODO: usage?
    add_help=False)
  eparser.set_defaults(subsubcommand='env')
  args.add_env(eparser, required=True)
  #args.add_role(eparser, required=True, labelled=True)
  args.add_cluster(eparser, required=True, labelled=True)

  # add help subsubparser to display info of subsubcommands
  hparser = subsubparsers.add_parser(
      'help',
      help='Prints help for commands',
      add_help = False)
  hparser._positionals.title = "Required arguments"
  hparser._optionals.title = "Optional arguments"
  hparser.add_argument(
      'help-subcommand',
      nargs = '?',
      default = 'help',
      help='Provide help for subcommands')
  hparser.set_defaults(subsubcommand='help')

  return show_parser

def run_help(command, parser, args, unknown_args):
  # get the command for detailed help
  command_help = args['help-subcommand']

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
    LOG.error("Unknown subcommand \'%s\'" % command_help)
    return False

def get_cluster_topologies(cluster):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_topologies(cluster))
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    LOG.error('Failed to retrive topologies running in cluster \'%s\'' % cluster)
    raise

def run_cluster(command, parser, cl_args, unknown_args):
  cluster = cl_args['cluster']
  try:
    result = get_cluster_topologies(cluster)[cluster]
  except Exception as ex:
    return False
  for env, topos in result.items():
    print('Environment \'%s\':' % env)
    for topo in topos[:10]:
      print("  %s" % topo)
    if len(topos) > 10:
      print("  ... with %d more topologies" % (len(topos) - 10))
  return True

def run_env(command, parser, cl_args, unknown_args):
  cluster, env = cl_args['cluster'], cl_args['env']
  try:
    result = get_cluster_topologies(cluster)[cluster][env]
  except Exception as ex:
    LOG.error("Failed to retrieve topologies under environment \'%s\'" % env)
    return False
  print('Topologies under cluster \'%s\' in environment \'%s\':' % (cluster, env))
  for env in result[:10]:
    print("  %s" % env)
  if len(result) > 10:
    print("  ... with %d more topologies" % (len(topos) - 10))
  return True

def run(command, parser, cl_args, unknown_args):
  parser = utils.get_subparser(parser, cl_args['subcommand'])
  subcommand = cl_args['subsubcommand']
  if subcommand == 'help':
    return run_help(command, parser, cl_args, unknown_args)
  if subcommand == 'cluster':
    return run_cluster(command, parser, cl_args, unknown_args)
  if subcommand == 'env':
    return run_env(command, parser, cl_args, unknown_args)
  return True

