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
import tornado.gen
import tornado.ioloop
from collections import defaultdict
from heron.common.src.python.color import Log
from heron.common.src.python.handler.access import heron as API
from tabulate import tabulate


def create_parser(subparsers):
  components_parser = subparsers.add_parser(
    'components',
    help='display information of a topology in a logical plan',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env(components_parser)
  args.add_topology_name(components_parser)
  args.add_verbose(components_parser)
  args.add_tracker_url(components_parser)
  args.add_config(components_parser)
  components_parser.set_defaults(subcommand='components')

  spouts_parser = subparsers.add_parser(
    'spouts',
    help='display information of spouts of a topology in a logical plan',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env(spouts_parser)
  args.add_topology_name(spouts_parser)
  args.add_verbose(spouts_parser)
  args.add_tracker_url(spouts_parser)
  args.add_config(spouts_parser)
  spouts_parser.set_defaults(subcommand='spouts')

  bolts_parser = subparsers.add_parser(
    'bolts',
    help='display information of bolts of a topology in a logical plan',
    usage="%(prog)s [options]",
    add_help=False)
  args.add_cluster_role_env(bolts_parser)
  args.add_topology_name(bolts_parser)
  args.add_verbose(bolts_parser)
  args.add_tracker_url(bolts_parser)
  args.add_config(bolts_parser)
  bolts_parser.set_defaults(subcommand='bolts')

  return subparsers


def get_logical_plan(cluster, env, topology, role):
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_logical_plan(cluster, env, topology, role))
  except Exception as ex:
    Log.error('Error: %s' % str(ex))
    Log.error('Failed to retrive logical plan info of topology \'%s\''
              % ('/'.join([cluster, role, env, topology])))
    raise


def parse_topo_loc(cl_args):
  try:
    topo_loc = cl_args['cluster/[role]/[env]'].split('/')
    topo_loc.append(cl_args['topology-name'])
    if len(topo_loc) != 4:
      raise
    return topo_loc
  except Exception:
    Log.error('Error: invalid topology location')
    raise


def dump_components(components):
  inputs, outputs = defaultdict(list), defaultdict(list)
  for ctype, component in components.iteritems():
    if ctype == 'bolts':
      for component_name, component_info in component.iteritems():
        for input_stream in component_info['inputs']:
          input_name = input_stream['component_name']
          inputs[component_name].append(input_name)
          outputs[input_name].append(component_name)
  info = []
  for ctype, component in components.iteritems():
    for component_name, component_info in component.iteritems():
      row = [ctype, component_name]
      row.append(','.join(inputs.get(component_name, ['-'])))
      row.append(','.join(outputs.get(component_name, ['-'])))
      info.append(row)
  header = ['type', 'name', 'input', 'output']
  print(tabulate(info, headers=header))
  return True


def dump_bolts(bolts, topo_loc):
  print('Bolts under topology \'%s\'' % '/'.join(topo_loc))
  for bolt in bolts.keys():
    print('  %s' % bolt)
  return True


def dump_spouts(spouts, topo_loc):
  print('Spouts under topology \'%s\'' % '/'.join(topo_loc))
  for spout in spouts.keys():
    print('  %s' % spout)
  return True


def run(cl_args, bolts_only, spouts_only):
  cluster, role, env = cl_args['cluster'], cl_args['role'], cl_args['environ']
  topology = cl_args['topology-name']
  topo_loc = [cluster, role, env, topology]
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
