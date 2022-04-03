#!/usr/bin/env python3
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
import sys
from collections import defaultdict
import requests

from tabulate import tabulate

from heron.common.src.python.utils.log import Log
from heron.tools.common.src.python.clients import tracker


def to_table(components, topo_info, component_filter):
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
    if component_filter == "spouts" and ctype != "spouts":
      continue
    if component_filter == "bolts" and ctype != "bolts":
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


def run(component_type: str, cluster: str, role: str, environment: str, topology: str):
  """ run command """
  try:
    components = tracker.get_logical_plan(cluster, environment, topology, role)
    topo_info = tracker.get_topology_info(cluster, environment, topology, role)
  except requests.ConnectionError as e:
    Log.error(f"Fail to connect to tracker: {e}")
    sys.exit(1)
  table, header = to_table(components, topo_info, component_type)
  print(tabulate(table, headers=header))
