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
from heron.ui.src.python.handlers.common.graph import Graph, TopologyDAG
import heron.explorer.src.python.args as args
from tabulate import tabulate
import json

LOG = logging.getLogger(__name__)

def create_parser(subparsers):
  components_parser = subparsers.add_parser(
    'components',
    help='display information of a topology in a logical plan',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_role_env_topo(components_parser)
  args.add_role(components_parser)
  components_parser.set_defaults(subcommand='components')

  spouts_parser = subparsers.add_parser(
    'spouts',
    help='display information of spouts of a topology in a logical plan',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_role_env_topo(spouts_parser)
  args.add_role(spouts_parser)
  spouts_parser.set_defaults(subcommand='spouts')

  bolts_parser = subparsers.add_parser(
    'bolts',
    help='display information of bolts of a topology in a logical plan',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_role_env_topo(bolts_parser)
  args.add_role(bolts_parser)
  bolts_parser.set_defaults(subcommand='bolts')

  return subparsers

def get_logical_plan(cluster, env, topology, role):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_logical_plan(cluster, env, topology, role))
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    LOG.error('Failed to retrive logical plan info of topology \'%s\''
              % ('/'.join([cluster, role, env, topology])))
    raise

def parse_topo_loc(cl_args):
  try:
    topo_loc = cl_args['[cluster]/[role]/[env]/[topology]'].split('/')
    if len(topo_loc) != 4:
      raise
    return topo_loc
  except Exception:
    LOG.error('Error: invalid topology location')
    raise

def dump_components(components):
  print(json.dumps(components, indent=4))
  return True

def dump_bolts(bolts, topo_loc):
  try:
    print('Bolts under topology \'%s\'' % '/'.join(topo_loc))
    for bolt in bolts.keys():
      print('  %s' % bolt)
    return True
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    raise ex

def dump_spouts(spouts, topo_loc):
  try:
    print('Spouts under topology \'%s\'' % '/'.join(topo_loc))
    for spout in spouts.keys():
      print('  %s' % spout)
    return True
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    raise ex

def run(cl_args, bolts_only, spouts_only):
  try:
    topo_loc = [cluster, role, env, topology] = parse_topo_loc(cl_args)
  except:
    return False
  try:
    components = get_logical_plan(cluster, env, topology, role)
    bolts, spouts = components["bolts"], components["spouts"]
    if not bolts_only and not spouts_only:
      return dump_components(components)
    if bolts_only:
      return dump_bolts(bolts, topo_loc)
    else:
      return dump_spouts(spouts, topo_loc)
  except:
    return False

def run_components(command, parser, cl_args, unknown_args):
  return run(cl_args, False, False)

def run_spouts(command, parser, cl_args, unknown_args):
  return run(cl_args, False, True)

def run_bolts(command, parser, cl_args, unknown_args):
  return run(cl_args, True, False)
