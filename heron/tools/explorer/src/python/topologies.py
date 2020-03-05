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

''' topologies.py '''
from heron.common.src.python.utils.log import Log
import heron.tools.explorer.src.python.args as args
import heron.tools.common.src.python.access.tracker_access as tracker_access
from tabulate import tabulate


def create_parser(subparsers):
  ''' create parser '''
  parser = subparsers.add_parser(
      'topologies',
      help='Display running topologies',
      usage="%(prog)s cluster/[role]/[env] [options]",
      add_help=True)
  args.add_cluster_role_env(parser)
  args.add_verbose(parser)
  args.add_tracker_url(parser)
  args.add_config(parser)
  parser.set_defaults(subcommand='topologies')
  return subparsers


def to_table(result):
  ''' normalize raw result to table '''
  max_count = 20
  table, count = [], 0
  for role, envs_topos in list(result.items()):
    for env, topos in list(envs_topos.items()):
      for topo in topos:
        count += 1
        if count > max_count:
          continue
        else:
          table.append([role, env, topo])
  header = ['role', 'env', 'topology']
  rest_count = 0 if count <= max_count else count - max_count
  return table, header, rest_count


# pylint: disable=superfluous-parens
def show_cluster(cl_args, cluster):
  ''' print topologies information to stdout '''
  try:
    result = tracker_access.get_cluster_topologies(cluster)
    if not result:
      Log.error('No topologies in cluster \'%s\'' % cluster)
      return False
    result = result[cluster]
  except Exception:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False
  table, header, rest_count = to_table(result)
  print('Topologies running in cluster \'%s\'' % cluster)
  if rest_count:
    print('  with %d more...' % rest_count)
  print(tabulate(table, headers=header))
  return True


# pylint: disable=superfluous-parens
def show_cluster_role(cl_args, cluster, role):
  ''' print topologies information to stdout '''
  try:
    result = tracker_access.get_cluster_role_topologies(cluster, role)
    if not result:
      Log.error('Unknown cluster/role \'%s\'' % '/'.join([cluster, role]))
      return False
    result = result[cluster]
  except Exception:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False
  table, header, rest_count = to_table(result)
  print('Topologies running in cluster \'%s\' submitted by \'%s\':' % (cluster, role))
  if rest_count:
    print('  with %d more...' % rest_count)
  print(tabulate(table, headers=header))
  return True


# pylint: disable=superfluous-parens
def show_cluster_role_env(cl_args, cluster, role, env):
  ''' print topologies information to stdout '''
  try:
    result = tracker_access.get_cluster_role_env_topologies(cluster, role, env)
    if not result:
      Log.error('Unknown cluster/role/env \'%s\'' % '/'.join([cluster, role, env]))
      return False
    result = result[cluster]
  except Exception:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False
  table, header, rest_count = to_table(result)
  print('Topologies running in cluster \'%s\', submitted by \'%s\', and\
 under environment \'%s\':' % (cluster, role, env))
  if rest_count:
    print('  with %d more...' % rest_count)
  print(tabulate(table, headers=header))
  return True

# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  """ run command """
  location = cl_args['cluster/[role]/[env]'].split('/')
  if len(location) == 1:
    return show_cluster(cl_args, *location)
  elif len(location) == 2:
    return show_cluster_role(cl_args, *location)
  elif len(location) == 3:
    return show_cluster_role_env(cl_args, *location)
  else:
    Log.error('Invalid topologies selection')
    return False
