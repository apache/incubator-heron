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

import heron.explorer.src.python.args as args
from heron.common.src.python.color import Log
from tabulate import tabulate
from heron.explorer.src.python.utils import get_cluster_topologies
from heron.explorer.src.python.utils import get_cluster_role_topologies
from heron.explorer.src.python.utils import get_cluster_role_env_topologies


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
    'topologies',
    help='Show running topologies',
    usage="%(prog)s [options]",
    add_help=True)
  args.add_cluster_role_env(parser)
  args.add_verbose(parser)
  args.add_tracker_url(parser)
  args.add_config(parser)
  parser.set_defaults(subcommand='topologies')
  return subparsers


# only working with updated tracker
def pp_table(result):
  max_count = 20
  info, count = [], 0
  for role, envs_topos in result.iteritems():
    for env, topos in envs_topos.iteritems():
      for topo in topos:
        count += 1
        if count > max_count:
          continue
        else:
          info.append([role, env, topo])
  header = ['role', 'env', 'topology']
  rest_count = 0 if count <= max_count else count - max_count
  return tabulate(info, headers=header), rest_count


def show_cluster(cluster):
  try:
    result = get_cluster_topologies(cluster)
    if not result:
      Log.error('Unknown cluster \'%s\'' % cluster)
      return False
    result = result[cluster]
  except Exception:
    return False
  table, rest_count = pp_table(result)
  print('Topologies running in cluster \'%s\'' % cluster)
  if rest_count:
    print('  with %d more...' % rest_count)
  print(table)
  return True


def show_cluster_role(cluster, role):
  try:
    result = get_cluster_role_topologies(cluster, role)
    if not result:
      Log.error('Unknown cluster/role \'%s\'' % '/'.join([cluster, role]))
      return False
    result = result[cluster]
  except Exception:
    return False
  table, rest_count = pp_table(result)
  print('Topologies running in cluster \'%s\' submitted by \'%s\':' % (cluster, role))
  if rest_count:
    print('  with %d more...' % rest_count)
  print(table)
  return True


def show_cluster_role_env(cluster, role, env):
  try:
    result = get_cluster_role_env_topologies(cluster, role, env)
    if not result:
      Log.error('Unknown cluster/role/env \'%s\'' % '/'.join([cluster, role, env]))
      return False
    result = result[cluster]
  except Exception:
    return False
  table, rest_count = pp_table(result)
  print('Topologies running in cluster \'%s\', submitted by \'%s\', and under environment \'%s\':' % (cluster, role, env))
  if rest_count:
    print('  with %d more...' % rest_count)
  print(table)
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
    Log.error('Invalid topology location')
    return False
