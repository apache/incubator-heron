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

''' logicalplan.py '''
from collections import defaultdict
from heron.common.src.python.utils.log import Log
import heron.tools.explorer.src.python.args as args
import heron.tools.common.src.python.access.tracker_access as tracker_access
from tabulate import tabulate


def create_parser(subparsers):
  """ create parser """
  components_parser = subparsers.add_parser(
      'components',
      help='Display information of a topology\'s components',
      usage="%(prog)s cluster/[role]/[env] topology-name [options]",
      add_help=False)
  args.add_cluster_role_env(components_parser)
  args.add_topology_name(components_parser)
  args.add_spouts(components_parser)
  args.add_bolts(components_parser)
  args.add_verbose(components_parser)
  args.add_tracker_url(components_parser)
  args.add_config(components_parser)
  components_parser.set_defaults(subcommand='components')

  return subparsers


# pylint: disable=misplaced-bare-raise
def parse_topo_loc(cl_args):
  """ parse topology location """
  try:
    topo_loc = cl_args['cluster/[role]/[env]'].split('/')
    topo_loc.append(cl_args['topology-name'])
    if len(topo_loc) != 4:
      raise
    return topo_loc
  except Exception:
    Log.error('Error: invalid topology location')
    raise


def to_table(components, topo_info):
  """ normalize raw logical plan info to table """
  inputs, outputs = defaultdict(list), defaultdict(list)
  for ctype, component in list(components.items()):
    if ctype == 'bolts':
      for component_name, component_info in list(component.items()):
        for input_stream in component_info['inputs']:
          input_name = input_stream['component_name']
          inputs[component_name].append(input_name)
          outputs[input_name].append(component_name)
  info = []
  spouts_instance = topo_info['physical_plan']['spouts']
  bolts_instance = topo_info['physical_plan']['bolts']
  for ctype, component in list(components.items()):
    # stages is an int so keep going
    if ctype == "stages":
      continue
    for component_name, component_info in list(component.items()):
      row = [ctype[:-1], component_name]
      if ctype == 'spouts':
        row.append(len(spouts_instance[component_name]))
      else:
        row.append(len(bolts_instance[component_name]))
      row.append(','.join(inputs.get(component_name, ['-'])))
      row.append(','.join(outputs.get(component_name, ['-'])))
      info.append(row)
  header = ['type', 'name', 'parallelism', 'input', 'output']
  return info, header


def filter_bolts(table, header):
  """ filter to keep bolts """
  bolts_info = []
  for row in table:
    if row[0] == 'bolt':
      bolts_info.append(row)
  return bolts_info, header


def filter_spouts(table, header):
  """ filter to keep spouts """
  spouts_info = []
  for row in table:
    if row[0] == 'spout':
      spouts_info.append(row)
  return spouts_info, header


# pylint: disable=unused-argument,superfluous-parens
def run(cl_args, compo_type):
  """ run command """
  cluster, role, env = cl_args['cluster'], cl_args['role'], cl_args['environ']
  topology = cl_args['topology-name']
  spouts_only, bolts_only = cl_args['spout'], cl_args['bolt']
  try:
    components = tracker_access.get_logical_plan(cluster, env, topology, role)
    topo_info = tracker_access.get_topology_info(cluster, env, topology, role)
    table, header = to_table(components, topo_info)
    if spouts_only == bolts_only:
      print(tabulate(table, headers=header))
    elif spouts_only:
      table, header = filter_spouts(table, header)
      print(tabulate(table, headers=header))
    else:
      table, header = filter_bolts(table, header)
      print(tabulate(table, headers=header))
    return True
  except:
    Log.error("Fail to connect to tracker: \'%s\'", cl_args["tracker_url"])
    return False



def run_components(command, parser, cl_args, unknown_args):
  """ run components command """
  return run(cl_args, 'all')


def run_spouts(command, parser, cl_args, unknown_args):
  """ run spouts command """
  return run(cl_args, 'spouts')


def run_bolts(command, parser, cl_args, unknown_args):
  """ run bolts command """
  return run(cl_args, 'bolts')
