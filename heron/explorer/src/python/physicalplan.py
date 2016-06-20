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

import sys
import logging
import tornado.gen
import tornado.ioloop
from heron.ui.src.python.handlers.access import heron as API
import heron.explorer.src.python.args as args
import heron.explorer.src.python.logicalplan as logicalplan
from tabulate import tabulate
import json

LOG = logging.getLogger(__name__)

def create_parser(subparsers):
  spouts_parser = subparsers.add_parser(
    'spouts',
    help = 'show info of a topology\'s spouts metrics',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_env_topo(spouts_parser)
  args.add_role(spouts_parser)
  spouts_parser.set_defaults(subcommand='spouts')

  bolts_parser = subparsers.add_parser(
   'bolts',
    help = 'show info of a topology\'s bolts metrics',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_env_topo(bolts_parser)
  args.add_role(bolts_parser)
  bolts_parser.set_defaults(subcommand='bolts')

  containers_parser = subparsers.add_parser(
    'containers',
    help = 'show info of a topology\'s containers metrics',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_env_topo(containers_parser)
  args.add_role(containers_parser)
  args.add_container_id(containers_parser)
  containers_parser.set_defaults(subcommand='containers')
  return subparsers

def parse_topo_loc(cl_args):
  try:
    topo_loc = cl_args['[cluster]/[env]/[topology]'].split('/')
    return topo_loc
  except Exception:
    LOG.error('Error: invalid topology location')
    raise

def get_topology_info(*args):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_topology_info(*args))
  except Exception as ex:
    LOG.error('Error: %s' % str(ex))
    raise

def get_topology_metrics(*args):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_comp_metrics(*args))
  except Exception as ex:
    raise

def get_component_metrics(component, cluster, env, topology):
  queries_normal = ['complete-latency', 'execute-latency', 'process-latency',
     'jvm-uptime-secs', 'jvm-process-cpu-load', 'jvm-memory-used-mb']
  count_queries_normal = ['emit-count', 'execute-count', 'ack-count', 'fail-count']
  queries = ['__%s' % m for m in queries_normal]
  count_queries = ['__%s/default' % m for m in count_queries_normal]
  all_queries = queries + count_queries
  m = dict(zip(queries, queries_normal) + zip(count_queries, count_queries_normal))
  try:
    result = get_topology_metrics(cluster, env, topology, component, [], all_queries, [0, -1])
  except:
    LOG.error("Failed to retrive metrics of component \'%s\'" % component)
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
  print(tabulate(stats, headers=headers))
  sys.stdout.flush()
  return True

def run_spouts(command, parser, cl_args, unknown_args):
  try:
    [cluster, env, topology] = parse_topo_loc(cl_args)
  except:
    return False

def run_containers(command, parser, cl_args, unknown_args):
  try:
    [cluster, env, topology] = parse_topo_loc(cl_args)
  except:
    return False
  container_id = cl_args['cid']
  result = get_topology_info(cluster, env, topology)
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


'''
def run_metrics(command, parser, cl_args, unknown_args):
  try:
    topo_loc = [cluster, env, topology] = cl_args['[cluster]/[env]/[topology]'].split('/')
  except Exception:

  [container_id, spout_name, bolt_name] = [cl_args[k] for k in ['cid', 'spout', 'bolt']]
  if spout_name is None and bolt_name is None:

    else:
      try:
        name = stmgrs[container_id]
      except Exception as ex:
        LOG.error("Error: %s" % str(ex))
        LOG.error("Unknown container ID \'%d\'" % container_id)
      container = containers[name]
      table = []
      for k in ["host", "pid", "id"]:
        table.append([k, container[k]])
      sys.stdout.flush()
      print(tabulate(table))
      return True
  elif spout_name is not None:
    return get_component_metrics(spout_name, *topo_loc)
  elif bolt_name is not None:
    return get_component_metrics(bolt_name, *topo_loc)
  else:
    LOG.error("Unknown error")
    return False
'''
