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
import sys
import tornado.gen
import tornado.ioloop
from heron.common.src.python.color import Log
from heron.common.src.python.handler.access import heron as API
from tabulate import tabulate


def create_parser(subparsers):
  spouts_parser = subparsers.add_parser(
    'spouts-metric',
    help='show info of a topology\'s spouts metrics',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env_topo(spouts_parser)
  args.add_spout_name(spouts_parser)
  spouts_parser.set_defaults(subcommand='spouts-metric')

  bolts_parser = subparsers.add_parser(
   'bolts-metric',
    help='show info of a topology\'s bolts metrics',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env_topo(bolts_parser)
  args.add_bolt_name(bolts_parser)
  bolts_parser.set_defaults(subcommand='bolts-metric')

  containers_parser = subparsers.add_parser(
    'containers',
    help='show info of a topology\'s containers metrics',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env_topo(containers_parser)
  args.add_container_id(containers_parser)
  containers_parser.set_defaults(subcommand='containers')
  return subparsers


def parse_topo_loc(cl_args):
  try:
    topo_loc = cl_args['cluster/role/env'].split('/')
    topo_name = cl_args['topology-name']
    topo_loc.append(topo_name)
    if len(topo_loc) != 4:
      raise
    return topo_loc
  except Exception:
    Log.error('Error: invalid topology location')
    raise


def get_topology_info(*args):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_topology_info(*args))
  except Exception as ex:
    Log.error(str(ex))
    raise


def get_topology_metrics(*args):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_comp_metrics(*args))
  except Exception as ex:
    Log.error(str(ex))
    raise


def get_component_metrics(component, cluster, env, topology, role):
  queries_normal = [
    'complete-latency', 'execute-latency', 'process-latency',
    'jvm-uptime-secs', 'jvm-process-cpu-load', 'jvm-memory-used-mb'
  ]
  count_queries_normal = ['emit-count', 'execute-count', 'ack-count', 'fail-count']
  queries = ['__%s' % m for m in queries_normal]
  count_queries = ['__%s/default' % m for m in count_queries_normal]
  all_queries = queries + count_queries
  m = dict(zip(queries, queries_normal) + zip(count_queries, count_queries_normal))
  try:
    result = get_topology_metrics(
      cluster, env, topology, component, [], all_queries, [0, -1], role)
  except:
    Log.error("Failed to retrive metrics of component \'%s\'" % component)
    return False
  metrics = result["metrics"]
  names = metrics.values()[0].keys()
  stats = []
  for n in names:
    info = [n]
    for field in all_queries:
      try:
        info.append(str(metrics[field][n]))
      except KeyError:
        pass
    stats.append(info)
  headers = ['container id'] + [m[k] for k in all_queries if k in metrics.keys()]
  sys.stdout.flush()
  print('\'%s\' metrics:' % component)
  print(tabulate(stats, headers=headers))
  sys.stdout.flush()
  return True


def run_spouts(command, parser, cl_args, unknown_args):
  try:
    [cluster, role, env, topology] = parse_topo_loc(cl_args)
  except:
    return False
  try:
    result = get_topology_info(cluster, env, topology, role)
    spouts = result['physical_plan']['spouts'].keys()
    spout_name = cl_args['spout']
    if spout_name in spouts:
      spouts = [spout_name]
    else:
      Log.error('Unknown spout: \'%s\'' % spout_name)
      raise
  except Exception:
    return False
  for spout in spouts:
    result = get_component_metrics(spout, cluster, env, topology, role)
    if not result:
      return False
  return True


def run_bolts(command, parser, cl_args, unknown_args):
  try:
    [cluster, role, env, topology] = parse_topo_loc(cl_args)
  except:
    return False
  try:
    result = get_topology_info(cluster, env, topology, role)
    bolts = result['physical_plan']['bolts'].keys()
    bolt_name = cl_args['bolt']
    if bolt_name in bolts:
      bolts = [bolt_name]
    else:
      Log.error('Unknown bolt: \'%s\'' % bolt_name)
      raise
  except Exception:
    return False
  for bolt in bolts:
    result = get_component_metrics(bolt, cluster, env, topology, role)
    if not result:
      return False
  return True


def run_containers(command, parser, cl_args, unknown_args):
  try:
    [cluster, role, env, topology] = parse_topo_loc(cl_args)
  except:
    return False
  container_id = cl_args['cid']
  result = get_topology_info(cluster, env, topology, role)
  containers = result['physical_plan']['stmgrs']
  all_bolts, all_spouts = set(), set()
  for container, bolts in result['physical_plan']['bolts'].items():
    all_bolts = all_bolts | set(bolts)
  for container, spouts in result['physical_plan']['spouts'].items():
    all_spouts = all_spouts | set(spouts)
  stmgrs = containers.keys()
  stmgrs.sort()
  if container_id is None:
    table = []
    for id, name in enumerate(stmgrs):
      cid = id + 1
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
