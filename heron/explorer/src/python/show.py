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
import heron.explorer.src.python.args as args
from heron.explorer.src.python.physicalplan import get_topology_info
import heron.explorer.src.python.help as help
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
  cluster_parser = subparsers.add_parser(
    'cluster',
    help='Show cluster info',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster(cluster_parser, required=True)
  args.add_role(cluster_parser)
  cluster_parser.set_defaults(subcommand='cluster')

  env_parser = subparsers.add_parser(
    'env',
    help='Show env info',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_env(env_parser, required=True)
  args.add_role(env_parser)

  env_parser.set_defaults(subcommand='env')
  return subparsers

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
  print(cl_args)
  try:
    [cluster, env] = cl_args['env'].split('/')
  except:
    print('Invalid environment %s' % cl_args['env'])
    return False
  try:
    result = get_cluster_topologies(cluster)[cluster][env]
  except:
    LOG.error("Failed to retrieve topologies under environment \'%s\'" % env)
    return False
  print('Topologies under cluster \'%s\' in environment \'%s\':' % (cluster, env))
  for env in result[:10]:
    print("  %s" % env)
  if len(result) > 10:
    print("  ... with %d more topologies" % (len(result) - 10))
  return True
