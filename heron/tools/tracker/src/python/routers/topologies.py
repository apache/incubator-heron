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
"""
These methods provide information about topologies based on information
from the state manager.

Some information may not be available for a topology due until the state
manager has recieved more information from the state manager.

> **TODO:** link to topology lifecycle documentation.

"""
from typing import List, Optional, Dict, Union

from heron.tools.tracker.src.python import state
from heron.tools.tracker.src.python.topology import (
    TopologyInfo,
    TopologyInfoExecutionState,
    TopologyInfoLogicalPlan,
    TopologyInfoMetadata,
    TopologyInfoPhysicalPlan,
    TopologyInfoSchedulerLocation,
)
from heron.tools.tracker.src.python.utils import EnvelopingAPIRouter

from fastapi import Query

router = EnvelopingAPIRouter()


@router.get("", response_model=Dict[str, Dict[str, List[str]]])
async def get_topologies(
    role: Optional[str] = Query(None), # what is rolve vs. name?
    cluster_names: List[str] = Query(None, alias="cluster"),
    environ_names: List[str] = Query(None, alias="environ"),
):
  """
  Return a map of (cluster, role) to a list of topology names.

  """
  result = {}
  for topology in state.tracker.filtered_topologies(cluster_names, environ_names, roles={role}):
    if topology.execution_state:
      t_role = topology.execution_state.role
    else:
      t_role = None
    result.setdefault(topology.cluster, {}).setdefault(t_role, []).append(
        topology.name
    )
  return result


@router.get(
    "/states",
    response_model=Dict[str, Dict[str, Dict[str, TopologyInfoExecutionState]]],
)
async def get_topologies_state(
    cluster_names: Optional[List[str]] = Query(None, alias="cluster"),
    environ_names: Optional[List[str]] = Query(None, alias="environ"),
    role: Optional[str] = Query(None, description="ignored", deprecated=True),
):
  """Return the execution states for topologies. Keyed by (cluster, environ, topology)."""
  result = {}

  for topology in state.tracker.filtered_topologies(cluster_names, environ_names, {}, {role}):
    topology_info = topology.info
    if topology_info is not None:
      result.setdefault(topology.cluster, {}).setdefault(topology.environ, {})[
          topology.name
      ] = topology_info.execution_state
  return result


@router.get("/info", response_model=TopologyInfo)
async def get_topology_info(
    cluster: str, role: str, environ: str, topology: str,
):
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  # TODO: 404 if no topology found
  # TODO: 412 if no topology info
  return topology.info


# XXX: this all smells like graphql
@router.get("/config", response_model=Dict[str, Union[int, str]])
async def get_topology_config(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  topology_info = topology.info
  return topology_info.physical_plan.config


@router.get("/physicalplan", response_model=TopologyInfoPhysicalPlan)
async def get_topology_physical_plan(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return topology.info.physical_plan


# Deprecated. See https://github.com/apache/incubator-heron/issues/1754
@router.get("/executionstate", response_model=TopologyInfoExecutionState)
async def get_topology_execution_state(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return topology.info.execution_state


@router.get("/schedulerlocation", response_model=TopologyInfoSchedulerLocation)
async def get_topology_scheduler_location(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return topology.info.scheduler_location


@router.get("/metadata", response_model=TopologyInfoMetadata)
async def get_topology_metadata(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return topology.info.metadata


@router.get("/logicalplan", response_model=TopologyInfoLogicalPlan)
async def get_topology_logical_plan(
    cluster: str, role: str, environ: str, topology: str,
):
  """
  This returns a transformed version of the logical plan, it probably
  shouldn't, especially with the renaming. The types should be fixed
  upstream, and the number of topology stages could find somewhere else
  to live.

  """
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  topology_info = topology.info
  return topology_info.logical_plan
