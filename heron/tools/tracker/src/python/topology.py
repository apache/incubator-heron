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

''' topology.py '''
import uuid

from typing import Callable, Dict, List, Optional

from heronpy.api import api_constants
from heron.common.src.python.utils.log import Log
from heron.proto.packing_plan_pb2 import PackingPlan
from heron.proto.physical_plan_pb2 import PhysicalPlan
from heron.proto.execution_state_pb2 import ExecutionState

# pylint: disable=too-many-instance-attributes
class Topology:
  """
  Class Topology
    Contains all the relevant information about
    a topology that its state manager has.

    All this info is fetched from state manager in one go.

    The watches are the callbacks that are called
    when there is any change in the topology
    instance using set_physical_plan, set_execution_state,
    set_tmanager, and set_scheduler_location. Any other means of changing will
    not call the watches.

  """

  def __init__(self, name: str, state_manager_name: str) -> None:
    self.name = name
    self.state_manager_name = state_manager_name
    self.physical_plan: PhysicalPlan = None
    self.packing_plan: PackingPlan = None
    self.execution_state: Optional[str] = None
    self.id: Optional[int] = None
    self.tmanager = None
    self.scheduler_location = None
    self.watches: Dict[uuid.UUID, Callable[[Topology], None]] = {}
    self._api_form: Optional[dict] = None

  def register_watch(self, callback: Callable[["Topology"], None]) -> Optional[uuid.UUID]:
    """
    Returns the UUID with which the watch is
    registered. This UUID can be used to unregister
    the watch.
    Returns None if watch could not be registered.

    The argument 'callback' must be a function that takes
    exactly one argument, the topology on which
    the watch was triggered.
    Note that the watch will be unregistered in case
    it raises any Exception the first time.

    This callback is also called at the time of registration.

    """
    # maybe this should use a counter
    # Generate a random UUID.
    uid = uuid.uuid4()
    if uid in self.watches:
      raise ValueError("Time to buy a lottery ticket")
    Log.info(f"Registering a watch with uid: {uid}")
    try:
      callback(self)
    except Exception as e:
      Log.error(f"Caught exception while triggering callback: {e}")
      Log.debug("source of error:", exc_info=True)
      return None
    self.watches[uid] = callback
    return uid

  def unregister_watch(self, uid) -> None:
    """
    Unregister the watch with the given UUID.
    """
    # Do not raise an error if UUID is not present in the watches
    Log.info(f"Unregister a watch with uid: {uid}")
    self.watches.pop(uid, None)

  def trigger_watches(self) -> None:
    """
    Call all the callbacks.

    If any callback raises an Exception, unregister the corresponding watch.

    """
    to_remove = []
    # the list() is in case the callbacks modify the watches
    for uid, callback in list(self.watches.items()):
      try:
        callback(self)
      except Exception as e:
        Log.error(f"Caught exception while triggering callback: {e}")
        Log.debug("source of error:", exc_info=True)
        to_remove.append(uid)

    for uid in to_remove:
      self.unregister_watch(uid)

  def set_physical_plan(self, physical_plan: PhysicalPlan) -> None:
    """ set physical plan """
    if not physical_plan:
      self.physical_plan = None
      self.id = None
    else:
      self.physical_plan = physical_plan
      self.id = physical_plan.topology.id
    self.trigger_watches()

  def set_packing_plan(self, packing_plan: PackingPlan) -> None:
    """ set packing plan """
    if not packing_plan:
      self.packing_plan = None
      self.id = None
    else:
      self.packing_plan = packing_plan
      self.id = packing_plan.id
    self.trigger_watches()

  def set_execution_state(self, execution_state: ExecutionState) -> None:
    """ set exectuion state """
    self.execution_state = execution_state
    self.trigger_watches()

  @property
  def cluster(self) -> Optional[str]:
    if self.execution_state:
      return self.execution_state.cluster
    return None

  @property
  def environ(self) -> Optional[str]:
    if self.execution_state:
      return self.execution_state.environ
    return None

  def set_tmanager(self, tmanager) -> None:
    """ set exectuion state """
    self.tmanager = tmanager
    self.trigger_watches()

  def set_scheduler_location(self, scheduler_location) -> None:
    """ set exectuion state """
    self.scheduler_location = scheduler_location
    self.trigger_watches()

  def num_instances(self) -> int:
    """
    Number of spouts + bolts
    """
    num = 0

    # Get all the components
    components = self.spouts() + self.bolts()

    # Get instances for each worker
    for component in components:
      config = component.comp.config
      for kvs in config.kvs:
        if kvs.key == api_constants.TOPOLOGY_COMPONENT_PARALLELISM:
          num += int(kvs.value)
          break

    return num

  def spouts(self):
    """
    Returns a list of Spout (proto) messages
    """
    if self.physical_plan:
      return list(self.physical_plan.topology.spouts)
    return []

  def spout_names(self):
    """
    Returns a list of names of all the spouts
    """
    return [component.comp.name for component in self.spouts()]

  def bolts(self):
    """
    Returns a list of Bolt (proto) messages
    """
    if self.physical_plan:
      return list(self.physical_plan.topology.bolts)
    return []

  def bolt_names(self):
    """
    Returns a list of names of all the bolts
    """
    return [component.comp.name for component in self.bolts()]

  def get_machines(self) -> List[str]:
    """
    Get all the machines that this topology is running on.
    These are the hosts of all the stmgrs.
    """
    if self.physical_plan:
      return [s.host_name for s in self.physical_plan.stmgrs]
    return []

  def get_status(self) -> str:
    """
    Get the current state of this topology.
    The state values are from the topology.proto
    RUNNING = 1, PAUSED = 2, KILLED = 3
    if the state is None "Unknown" is returned.
    """
    status = None
    if self.physical_plan and self.physical_plan.topology:
      status = self.physical_plan.topology.state

    return {
        1: "Running",
        2: "Paused",
        3: "Killed",
    }.get(status, "Unknown")
