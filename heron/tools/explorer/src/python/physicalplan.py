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

from typing import Optional

import requests

from tabulate import tabulate

from heron.common.src.python.utils.log import Log
from heron.tools.common.src.python.clients import tracker


def to_table(metrics):
  """ normalize raw metrics API result to table """
  all_queries = tracker.metric_queries()
  m = tracker.queries_map()
  header = ['container id'] + [m[k] for k in all_queries if k in metrics.keys()]
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
    component: Optional[str],
) -> None:
  """Render a table of metrics."""
  try:
    result = tracker.get_topology_info(cluster, environment, topology, role)
  except requests.ConnectionError as e:
    Log.error(f"Fail to connect to tracker: {e}")
    sys.exit(1)

  all_components = sorted(result['physical_plan']['components'].keys())
  if component:
    if component not in all_components:
      Log.error(f"Unknown component: {component!r}")
      sys.exit(1)
    components = [component]
  else:
    components = all_components
  all_queries = tracker.metric_queries()

  for i, comp in enumerate(components):
    try:
      result = tracker.get_comp_metrics(
          cluster, environment, topology, comp, [], all_queries, [0, -1], role,
      )
    except requests.ConnectionError as e:
      Log.error(f"Fail to connect to tracker: {e}")
      sys.exit(1)
    stat, header = to_table(result["metrics"])
    if i != 0:
      print('')
    print(f"{comp!r} metrics:")
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
    result = tracker.get_topology_info(cluster, environment, topology, role)
  except requests.ConnectionError as e:
    Log.error(f"Fail to connect to tracker: {e}")
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
