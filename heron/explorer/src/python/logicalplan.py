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
import json

LOG = logging.getLogger(__name__)

def create_parser(subparsers):
  parser = subparsers.add_parser(
    'components',
    help='display information of a topology in a logical plan',
    usage = "%(prog)s [options]",
    add_help = False)

  args.add_cluster_env_topo(parser)
  args.add_spouts(parser)
  args.add_bolts(parser)
  args.add_verbose(parser)
  parser.set_defaults(subcommand='components')
  return parser

def get_logical_plan(cluster, env, topology):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_logical_plan(cluster, env, topology))
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    LOG.error('Failed to retrive logical plan info of topology \'%s\''
              % ('/'.join([cluster, env, topology])))
    raise

def get_components(cl_args):
  try:
    topo_loc = cl_args['[cluster]/[env]/[topology]']
    [cluster, env, topology] = topo_loc.split('/')
  except Exception:
    LOG.error('Error: invalid topology location \'%s\'' % topo_loc)
    raise
  try:
    return get_logical_plan(cluster, env, topology), topo_loc
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    raise

def dump_bolts(bolts, topo_loc):
  try:
    print('Bolts under topology \'%s\'' % topo_loc)
    for bolt in bolts.keys():
      print('  %s' % bolt)
    return True
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    raise ex

def dump_spouts(spouts, topo_loc):
  try:
    print('Spouts under topology \'%s\'' % topo_loc)
    for spout in spouts.keys():
      print('  %s' % spout)
    return True
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    raise ex

def run(command, parser, cl_args, unknown_args):
  try:
    print(cl_args)
    components, topo_loc = get_components(cl_args)
    show_bolts, show_spouts = cl_args['bolt'], cl_args['spout']
    bolts, spouts = components["bolts"], components["spouts"]
    if not show_bolts and not show_spouts:
      # should display some ascii topology
      # but Python does not have a handy library for doing this
      str(TopologyDAG(components))
      return False
    else:
      if show_bolts: dump_bolts(bolts, topo_loc)
      if show_spouts: dump_spouts(spouts, topo_loc)
      return True
  except Exception as ex:
    print('Error: %s' % str(ex))
