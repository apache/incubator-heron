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

''' filestatemanager.py '''
import os
import threading

from collections import defaultdict

from heron.statemgrs.src.python.statemanager import StateManager

from heron.proto.execution_state_pb2 import ExecutionState
from heron.proto.packing_plan_pb2 import PackingPlan
from heron.proto.physical_plan_pb2 import PhysicalPlan
from heron.proto.scheduler_pb2 import SchedulerLocation
from heron.proto.tmaster_pb2 import TMasterLocation
from heron.proto.topology_pb2 import Topology

# pylint: disable=too-many-instance-attributes
class FileStateManager(StateManager):
  """
  State manager which reads states from local file system.
  This is not a production level state manager. The watches
  are based on polling the file system at regular intervals.
  """

  def __init__(self, name, rootpath):
    self.name = name
    self.rootpath = rootpath

    # This is the cache of the state directories.
    self.topologies_directory = {}
    self.execution_state_directory = {}
    self.packing_plan_directory = {}
    self.pplan_directory = {}
    self.tmaster_directory = {}
    self.scheduler_location_directory = {}

    # The watches are triggered when there
    # is a corresponding change.
    # The list contains the callbacks to be called
    # when topologies change.
    self.topologies_watchers = []

    # The dictionary is from the topology name
    # to the callback.
    self.topology_watchers = defaultdict(lambda: [])
    self.execution_state_watchers = defaultdict(lambda: [])
    self.packing_plan_watchers = defaultdict(lambda: [])
    self.pplan_watchers = defaultdict(lambda: [])
    self.tmaster_watchers = defaultdict(lambda: [])
    self.scheduler_location_watchers = defaultdict(lambda: [])

    # Instantiate the monitoring thread.
    self.monitoring_thread = threading.Thread(target=self.monitor)

    # cancellable sleep
    self.event = threading.Event()

  # pylint: disable=attribute-defined-outside-init
  def start(self):
    """ start monitoring thread """
    self.monitoring_thread_stop_signal = False
    self.monitoring_thread.start()

  def stop(self):
    """" stop monitoring thread """
    self.monitoring_thread_stop_signal = True
    self.event.set()

  def monitor(self):
    """
    Monitor the rootpath and call the callback
    corresponding to the change.
    This monitoring happens periodically. This function
    is called in a seperate thread from the main thread,
    because it sleeps for the intervals between each poll.
    """

    def trigger_watches_based_on_files(watchers, path, directory, ProtoClass):
      """
      For all the topologies in the watchers, check if the data
      in directory has changed. Trigger the callback if it has.
      """
      for topology, callbacks in list(watchers.items()):
        file_path = os.path.join(path, topology)
        data = ""
        if os.path.exists(file_path):
          with open(os.path.join(path, topology)) as f:
            data = f.read()
        if topology not in directory or data != directory[topology]:
          proto_object = ProtoClass()
          proto_object.ParseFromString(data)
          for callback in callbacks:
            callback(proto_object)
          directory[topology] = data

    while not self.monitoring_thread_stop_signal:
      topologies_path = self.get_topologies_path()

      topologies = []
      if os.path.isdir(topologies_path):
        topologies = list([f for f in os.listdir(topologies_path)
                           if os.path.isfile(os.path.join(topologies_path, f))])
      if set(topologies) != set(self.topologies_directory):
        for callback in self.topologies_watchers:
          callback(topologies)
      self.topologies_directory = topologies

      trigger_watches_based_on_files(
          self.topology_watchers, topologies_path, self.topologies_directory, Topology)

      # Get the directory name for execution state
      execution_state_path = os.path.dirname(self.get_execution_state_path(""))
      trigger_watches_based_on_files(
          self.execution_state_watchers, execution_state_path,
          self.execution_state_directory, ExecutionState)

      # Get the directory name for packing_plan
      packing_plan_path = os.path.dirname(self.get_packing_plan_path(""))
      trigger_watches_based_on_files(
          self.packing_plan_watchers, packing_plan_path, self.packing_plan_directory, PackingPlan)

      # Get the directory name for pplan
      pplan_path = os.path.dirname(self.get_pplan_path(""))
      trigger_watches_based_on_files(
          self.pplan_watchers, pplan_path,
          self.pplan_directory, PhysicalPlan)

      # Get the directory name for tmaster
      tmaster_path = os.path.dirname(self.get_tmaster_path(""))
      trigger_watches_based_on_files(
          self.tmaster_watchers, tmaster_path,
          self.tmaster_directory, TMasterLocation)

      # Get the directory name for scheduler location
      scheduler_location_path = os.path.dirname(self.get_scheduler_location_path(""))
      trigger_watches_based_on_files(
          self.scheduler_location_watchers, scheduler_location_path,
          self.scheduler_location_directory, SchedulerLocation)

      # Sleep for some time
      self.event.wait(timeout=5)

  def get_topologies(self, callback=None):
    """get topologies"""
    if callback:
      self.topologies_watchers.append(callback)
    else:
      topologies_path = self.get_topologies_path()
      return [f for f in os.listdir(topologies_path)
              if os.path.isfile(os.path.join(topologies_path, f))]

  def get_topology(self, topologyName, callback=None):
    """get topology"""
    if callback:
      self.topology_watchers[topologyName].append(callback)
    else:
      topology_path = self.get_topology_path(topologyName)
      with open(topology_path) as f:
        data = f.read()
        topology = Topology()
        topology.ParseFromString(data)
        return topology

  def create_topology(self, topologyName, topology):
    """
    Create path is currently not supported in file based state manager.
    """
    pass

  def delete_topology(self, topologyName):
    """
    Delete path is currently not supported in file based state manager.
    """
    pass

  def get_packing_plan(self, topologyName, callback=None):
    """ get packing plan """
    if callback:
      self.packing_plan_watchers[topologyName].append(callback)
    else:
      packing_plan_path = self.get_packing_plan_path(topologyName)
      with open(packing_plan_path) as f:
        data = f.read()
        packing_plan = PackingPlan()
        packing_plan.ParseFromString(data)

  def get_pplan(self, topologyName, callback=None):
    """
    Get physical plan of a topology
    """
    if callback:
      self.pplan_watchers[topologyName].append(callback)
    else:
      pplan_path = self.get_pplan_path(topologyName)
      with open(pplan_path) as f:
        data = f.read()
        pplan = PhysicalPlan()
        pplan.ParseFromString(data)
        return pplan

  def create_pplan(self, topologyName, pplan):
    """
    Create path is currently not supported in file based state manager.
    """
    pass

  def delete_pplan(self, topologyName):
    """
    Delete path is currently not supported in file based state manager.
    """
    pass

  def get_execution_state(self, topologyName, callback=None):
    """
    Get execution state
    """
    if callback:
      self.execution_state_watchers[topologyName].append(callback)
    else:
      execution_state_path = self.get_execution_state_path(topologyName)
      with open(execution_state_path) as f:
        data = f.read()
        executionState = ExecutionState()
        executionState.ParseFromString(data)
        return executionState

  def create_execution_state(self, topologyName, executionState):
    """
    Create path is currently not supported in file based state manager.
    """
    pass

  def delete_execution_state(self, topologyName):
    """
    Delete path is currently not supported in file based state manager.
    """
    pass

  def get_tmaster(self, topologyName, callback=None):
    """
    Get tmaster
    """
    if callback:
      self.tmaster_watchers[topologyName].append(callback)
    else:
      tmaster_path = self.get_tmaster_path(topologyName)
      with open(tmaster_path) as f:
        data = f.read()
        tmaster = TMasterLocation()
        tmaster.ParseFromString(data)
        return tmaster

  def get_scheduler_location(self, topologyName, callback=None):
    """
    Get scheduler location
    """
    if callback:
      self.scheduler_location_watchers[topologyName].append(callback)
    else:
      scheduler_location_path = self.get_scheduler_location_path(topologyName)
      with open(scheduler_location_path) as f:
        data = f.read()
        scheduler_location = SchedulerLocation()
        scheduler_location.ParseFromString(data)
        return scheduler_location
