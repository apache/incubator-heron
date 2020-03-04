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

''' physicalplan.py '''
import sys
from heron.common.src.python.utils.log import Log
import heron.tools.common.src.python.access.tracker_access as tracker_access
import heron.tools.explorer.src.python.args as args
from tabulate import tabulate


def create_parser(subparsers):
  """ create parser """
  metrics_parser = subparsers.add_parser(
      'metrics',
      help='Display info of a topology\'s metrics',
      usage="%(prog)s cluster/[role]/[env] topology-name [options]",
      add_help=False)
  args.add_cluster_role_env(metrics_parser)
  args.add_topology_name(metrics_parser)
  args.add_verbose(metrics_parser)
  args.add_tracker_url(metrics_parser)
  args.add_config(metrics_parser)
  args.add_component_name(metrics_parser)
  metrics_parser.set_defaults(subcommand='metrics')

  containers_parser = subparsers.add_parser(
      'containers',
      help='Display info of a topology\'s containers metrics',
      usage="%(prog)s cluster/[role]/[env] topology-name [options]",
      add_help=False)
  args.add_cluster_role_env(containers_parser)
  args.add_topology_name(containers_parser)
  args.add_verbose(containers_parser)
  args.add_tracker_url(containers_parser)
  args.add_config(containers_parser)
  args.add_container_id(containers_parser)
  containers_parser.set_defaults(subcommand='containers')

  return subparsers


# pylint: disable=misplaced-bare-raise
def parse_topo_loc(cl_args):
  """ parse topology location """
  try:
    topo_loc = cl_args['cluster/[role]/[env]'].split('/')
    topo_name = cl_args['topology-name']
    topo_loc.append(topo_name)
    if len(topo_loc) != 4:
      raise
    return topo_loc
  except Exception:
    Log.error('Invalid topology location')
    raise


def to_table(metrics):
  """ normalize raw metrics API result to table """
  all_queries = tracker_access.metric_queries()
  m = tracker_access.queries_map()
  names = list(metrics.values())[0].keys()
  stats = []
  for n in names:
    info = [n]
    for field in all_queries:
      try:
        info.append(str(metrics[field][n]))
      except KeyError:
        pass
    stats.append(info)
  header = ['container id'] + [m[k] for k in all_queries if k in list(metrics.keys())]
  return stats, header


# pylint: disable=unused-argument,superfluous-parens
def run_metrics(command, parser, cl_args, unknown_args):
  """ run metrics subcommand """
  cluster, role, env = cl_args['cluster'], cl_args['role'], cl_args['environ']
  topology = cl_args['topology-name']
  try:
    result = tracker_access.get_topology_info(cluster, env, topology, role)
    spouts = list(result['physical_plan']['spouts'].keys())
    bolts = list(result['physical_plan']['bolts'].keys())
    components = spouts + bolts
    cname = cl_args['component']
    if cname:
      if cname in components:
        components = [cname]
      else:
        Log.error('Unknown component: \'%s\'' % cname)
        raise
  except Exception:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False
  cresult = []
  for comp in components:
    try:
      metrics = tracker_access.get_component_metrics(comp, cluster, env, topology, role)
    except:
      Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
      return False
    stat, header = to_table(metrics)
    cresult.append((comp, stat, header))
  for i, (comp, stat, header) in enumerate(cresult):
    if i != 0:
      print('')
    print('\'%s\' metrics:' % comp)
    print(tabulate(stat, headers=header))
  return True


# pylint: disable=unused-argument,superfluous-parens
def run_bolts(command, parser, cl_args, unknown_args):
  """ run bolts subcommand """
  cluster, role, env = cl_args['cluster'], cl_args['role'], cl_args['environ']
  topology = cl_args['topology-name']
  try:
    result = tracker_access.get_topology_info(cluster, env, topology, role)
    bolts = list(result['physical_plan']['bolts'].keys())
    bolt_name = cl_args['bolt']
    if bolt_name:
      if bolt_name in bolts:
        bolts = [bolt_name]
      else:
        Log.error('Unknown bolt: \'%s\'' % bolt_name)
        raise
  except Exception:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False
  bolts_result = []
  for bolt in bolts:
    try:
      metrics = tracker_access.get_component_metrics(bolt, cluster, env, topology, role)
      stat, header = to_table(metrics)
      bolts_result.append((bolt, stat, header))
    except Exception:
      Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
      return False
  for i, (bolt, stat, header) in enumerate(bolts_result):
    if i != 0:
      print('')
    print('\'%s\' metrics:' % bolt)
    print(tabulate(stat, headers=header))
  return True

# pylint: disable=too-many-locals,superfluous-parens
def run_containers(command, parser, cl_args, unknown_args):
  """ run containers subcommand """
  cluster, role, env = cl_args['cluster'], cl_args['role'], cl_args['environ']
  topology = cl_args['topology-name']
  container_id = cl_args['id']
  try:
    result = tracker_access.get_topology_info(cluster, env, topology, role)
  except:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False
  containers = result['physical_plan']['stmgrs']
  all_bolts, all_spouts = set(), set()
  for _, bolts in list(result['physical_plan']['bolts'].items()):
    all_bolts = all_bolts | set(bolts)
  for _, spouts in list(result['physical_plan']['spouts'].items()):
    all_spouts = all_spouts | set(spouts)
  stmgrs = list(containers.keys())
  stmgrs.sort()
  if container_id is not None:
    try:
      normalized_cid = container_id - 1
      if normalized_cid < 0:
        raise
      stmgrs = [stmgrs[normalized_cid]]
    except:
      Log.error('Invalid container id: %d' % container_id)
      return False
  table = []
  for sid, name in enumerate(stmgrs):
    cid = sid + 1
    host = containers[name]["host"]
    port = containers[name]["port"]
    pid = containers[name]["pid"]
    instances = containers[name]["instance_ids"]
    bolt_nums = len([instance for instance in instances if instance in all_bolts])
    spout_nums = len([instance for instance in instances if instance in all_spouts])
    table.append([cid, host, port, pid, bolt_nums, spout_nums, len(instances)])
  headers = ["container", "host", "port", "pid", "#bolt", "#spout", "#instance"]
  sys.stdout.flush()
  print(tabulate(table, headers=headers))
  return True
