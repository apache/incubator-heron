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

''' physicalplan.py '''
import sys
from heron.common.src.python.utils.log import Log
import heron.tools.common.src.python.access.tracker_access as tracker_access
from tabulate import tabulate


def to_table(metrics):
  """ normalize raw metrics API result to table """
  all_queries = tracker_access.metric_queries()
  m = tracker_access.queries_map()
  header = ['container id'] + [m[k] for k in all_queries if k in list(metrics.keys())]
  stats = []
  if not metrics:
    return stats, header
  names = list(metrics.values())[0].keys()
  for n in names:
    info = [n]
    for field in all_queries:
      try:
        info.append(str(metrics[field][n]))
      except KeyError:
        pass
    stats.append(info)
  return stats, header


def run_metrics(
    cluster: str,
    role: str,
    environment: str,
    topology: str,
    component: str,
) -> None:
  """Render a table of metrics."""
  try:
    result = tracker_access.get_topology_info(cluster, environment, topology, role)
  except Exception:
    Log.error("Fail to connect to tracker")
    sys.exit(1)
  spouts = list(result['physical_plan']['spouts'].keys())
  bolts = list(result['physical_plan']['bolts'].keys())
  components = spouts + bolts
  if component:
    if component in components:
      components = [component]
    else:
      Log.error(f"Unknown component: {component!r}")
      sys.exit(1)
  cresult = []
  for comp in components:
    try:
      metrics = tracker_access.get_component_metrics(comp, cluster, environment, topology, role)
    except:
      Log.error("Fail to connect to tracker")
      sys.exit(1)
    stat, header = to_table(metrics)
    cresult.append((comp, stat, header))
  for i, (c, stat, header) in enumerate(cresult):
    if i != 0:
      print('')
    print(f"{c!r} metrics:")
    print(tabulate(stat, headers=header))

def run_containers(
    cluster: str,
    role: str,
    environment: str,
    topology: str,
    container_id: str,
) -> None:
  """Render a table of container information."""
  try:
    result = tracker_access.get_topology_info(cluster, environment, topology, role)
  except:
    Log.error("Fail to connect to tracker")
    sys.exit(1)
  containers = result['physical_plan']['stmgrs']
  all_bolts, all_spouts = set(), set()
  for bolts in result['physical_plan']['bolts'].values():
    all_bolts |= set(bolts)
  for spouts in result['physical_plan']['spouts'].values():
    all_spouts |= set(spouts)
  stmgrs = sorted(containers.keys())
  if container_id is not None:
    stmgrs = [stmgrs[container_id]]
  table = []
  for cid, name in enumerate(stmgrs, (container_id + 1 if container_id else 1)):
    instances = containers[name]["instance_ids"]
    table.append([
        cid,
        containers[name]["host"],
        containers[name]["port"],
        containers[name]["pid"],
        len([1 for instance in instances if instance in all_bolts]),
        len([1 for instance in instances if instance in all_spouts]),
        len(instances),
    ])
  headers = ["container", "host", "port", "pid", "#bolt", "#spout", "#instance"]
  print(tabulate(table, headers=headers))
