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
from heron.common.src.python.color import Log
from heron.common.src.python.handler.access import heron as API
import heron.explorer.src.python.args as args
import json

# subsubparsers for roles and env are currently not supported
# because of design of Heron tracker API
# Tracker API does not have the concepts of roles
# see ``getTopologyInfo`` in ``heron/tracer/src/python/tracker.py``
# this function simply returns the first topology it finds that
# matches cluster/env/topology_name. It is possible that two different
# users may submit topologies of same names
# Bill: might not that important in real production since it rarely happens
def create_parser(subparsers):
  parser = subparsers.add_parser(
    'show',
    help='Show running topologies',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env(parser)
  parser.set_defaults(subcommand='show')

  return subparsers

def get_cluster_topologies(cluster):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    result = instance.run_sync(lambda: API.get_cluster_topologies(cluster))
    if not result: raise
  except Exception as ex:
    Log.error('Failed to retrive topologies running in cluster \'%s\'' % cluster)
    raise

def get_cluster_role_topologies(cluster, role):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_role_topologies(cluster, role))
  except Exception as ex:
    Log.error('Error: %s' % str(ex))
    Log.error('Failed to retrive topologies running in cluster'
              '\'%s\' submitted by %s' % (cluster, role))
    raise

def get_cluster_role_env_topologies(cluster, role, env):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_role_env_topologies(cluster, role, env))
  except Exception as ex:
    Log.error('Error: %s' % str(ex))
    Log.error('Failed to retrive topologies running in cluster'
              '\'%s\' submitted by %s under environment %s' % (cluster, role, env))
    raise

def show_cluster(cluster):
  try:
    result = get_cluster_topologies(cluster)
    Log.info(result)
    result = result[cluster]
  except Exception as ex:
    return False
  for env, topos in result.iteritems():
    print('Environment \'%s\':' % env)
    for topo in topos[:10]:
      print("  %s" % topo)
    if len(topos) > 10:
      print("  ... with %d more topologies" % (len(topos) - 10))
  return True

def show_cluster_role(cluster, role):
  try:
    result = get_cluster_role_topologies(cluster, role)
  except:
    return False
  print('Topologies under cluster \'%s\' submitted by \'%s\':' % (cluster, role))

  for env, topo_names in result[cluster].iteritems():
    print('Environment \'%s\':' % env)
    for topo_name in topo_names:
      print('  %s' % topo_name)
  return True

def show_cluster_role_env(cluster, role, env):
  try:
    result = get_cluster_role_env_topologies(cluster, role, env)
  except:
    return False
  print('Topologies under cluster \'%s\' submitted by \'%s\':' % (cluster, role))
  print(json.dumps(result, indent=4))
  for env, topo_names in result[cluster].iteritems():
    print('Environment \'%s\':' % env)
    for topo_name in topo_names:
      print('  %s' % topo_name)
  return True

def run(command, parser, cl_args, unknown_args):
  location = cl_args['cluster/[role]/[env]'].split('/')
  if len(location) == 0:
    Log.error('Invalid topology selection')
    return False
  elif len(location) == 1:
    return show_cluster(*location)
  elif len(location) == 2:
    return show_cluster_role(*location)
  elif len(location) == 3:
    return show_cluster_role_env(*location)
  else:
    return False

