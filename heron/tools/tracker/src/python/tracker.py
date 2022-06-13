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

''' tracker.py '''
import threading
import sys

from functools import partial
from typing import Any, Container, List, Optional

from heron.common.src.python.utils.log import Log
from heron.statemgrs.src.python import statemanagerfactory
from heron.statemgrs.src.python.statemanager import StateManager
from heron.tools.tracker.src.python.config import Config
from heron.tools.tracker.src.python.topology import Topology


class Tracker:
  """
  Tracker is a stateless cache of all the topologies
  for the given state managers. It watches for
  any changes on the topologies, like submission,
  killing, movement of containers, etc..

  This class caches all the data and is accessed
  by handlers.
  """

  __slots__ = ["topologies", "config", "state_managers", "lock"]

  def __init__(self, config: Config):
    self.config = config
    self.topologies: List[Topology] = []
    self.state_managers = []
    self.lock = threading.RLock()

  def sync_topologies(self) -> None:
    """
    Sync the topologies with the statemgrs.
    """
    self.state_managers = statemanagerfactory.get_all_state_managers(self.config.statemgr_config)
    try:
      for state_manager in self.state_managers:
        state_manager.start()
    except Exception as e:
      Log.critical(f"Found exception while initializing state managers: {e}", exc_info=True)
      sys.exit(1)

    def on_topologies_watch(state_manager: StateManager, topologies: List[str]) -> None:
      """watch topologies"""
      topologies = set(topologies)
      Log.debug("Received topologies: %s, %s", state_manager.name, topologies)
      cached_names = {t.name for t in self.get_stmgr_topologies(state_manager.name)}
      Log.debug(f"Existing topologies: {state_manager.name}, {cached_names}")
      for name in cached_names:
        if name not in topologies:
          Log.info(f"Removing topology: {name} in rootpath: {state_manager.rootpath}")
          self.remove_topology(name, state_manager.name)

      for name in topologies:
        if name not in cached_names:
          self.add_new_topology(state_manager, name)

    for state_manager in self.state_managers:
      # The callback function with the bound state_manager as first variable
      state_manager.get_topologies(partial(on_topologies_watch, state_manager))

  def stop_sync(self) -> None:
    for state_manager in self.state_managers:
      state_manager.stop()

  def get_topology(
      self,
      cluster: str,
      role: Optional[str],
      environ: str,
      topology_name: str,
  ) -> Any:
    """
    Find and return the topology given its cluster, environ, topology name, and
    an optional role.
    Raises exception if topology is not found, or more than one are found.
    """
    topologies = [t for t in self.topologies if t.name == topology_name
                  and t.cluster == cluster
                  and (not role or t.execution_state.role == role)
                  and t.environ == environ]
    if len(topologies) != 1:
      if role is not None:
        raise KeyError(f"Topology not found for {cluster}, {role}, {environ}, {topology_name}")
      raise KeyError(f"Topology not found for {cluster}, {environ}, {topology_name}")

    # There is only one topology which is returned.
    return topologies[0]

  def get_stmgr_topologies(self, name: str) -> List[Any]:
    """
    Returns all the topologies for a given state manager.
    """
    return [t for t in self.topologies if t.state_manager_name == name]

  def add_new_topology(self, state_manager: StateManager, topology_name: str) -> None:
    """
    Adds a topology in the local cache, and sets a watch
    on any changes on the topology.
    """
    topology = Topology(topology_name, state_manager.name, self.config)
    with self.lock:
      if topology not in self.topologies:
        Log.info(f"Adding new topology: {topology_name}, state_manager: {state_manager.name}")
        self.topologies.append(topology)

    # Set watches on the pplan, execution_state, tmanager and scheduler_location.
    state_manager.get_pplan(topology_name, topology.set_physical_plan)
    state_manager.get_packing_plan(topology_name, topology.set_packing_plan)
    state_manager.get_execution_state(topology_name, topology.set_execution_state)
    state_manager.get_tmanager(topology_name, topology.set_tmanager)
    state_manager.get_scheduler_location(topology_name, topology.set_scheduler_location)

  def remove_topology(self, topology_name: str, state_manager_name: str) -> None:
    """
    Removes the topology from the local cache.
    """
    with self.lock:
      self.topologies = [
          topology
          for topology in self.topologies
          if not (
              topology.name == topology_name
              and topology.state_manager_name == state_manager_name
          )
      ]

  def filtered_topologies(
      self,
      clusters: Container[str],
      environs: Container[str],
      names: Container[str],
      roles: Container[str], # should deprecate?
    ) -> List[Topology]:
    """
    Return a filtered copy of the topologies which have the given properties.

    If a filter is falsy (i.e. empty) then all topologies will match on that property.

    """
    return [
        topology
        for topology in self.topologies[:]
        if (
            (not clusters or topology.cluster in clusters)
            and (not environs or topology.environ in environs)
            and (not names or topology.name in names)
            and (not roles or (topology.execution_state and topology.execution_state.role))
        )
    ]
