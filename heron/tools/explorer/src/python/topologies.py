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

''' topologies.py '''
import sys

from tabulate import tabulate

import requests

from heron.common.src.python.utils.log import Log
from heron.tools.common.src.python.clients import tracker


def to_table(result):
  table = []
  for role, envs_topos in result.items():
    for env, topos in envs_topos.items():
      for topo in topos:
        table.append([role, env, topo])
  header = ['role', 'env', 'topology']
  return table, header


def run(cre: str) -> None:
  """Print all topologies under the given CRE."""
  cluster, *role_env = cre.split('/')
  if len(role_env) > 2:
    Log.error("Invalid topologies selection")
    sys.exit(1)
  try:
    result = tracker.get_topologies(cluster, *role_env)
  except requests.ConnectionError as e:
    Log.error(f"Fail to connect to tracker: {e}")
    sys.exit(1)
  topologies = result[cluster]
  table, header = to_table(topologies)
  print(f"Topologies in {cre}:")
  print(tabulate(table, headers=header))
