import os
import sys
import threading
import time
import traceback

from collections import defaultdict

from heron.state.src.python.statemanager import StateManager
from heron.state.src.python.stateexceptions import StateException

from heron.proto.execution_state_pb2 import ExecutionState
from heron.proto.physical_plan_pb2 import PhysicalPlan
from heron.proto.tmaster_pb2 import TMasterLocation
from heron.proto.topology_pb2 import Topology

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
    self.pplan_directory = {}
    self.tmaster_directory = {}

    # The watches are triggered when there
    # is a corresponding change.
    # The list contains the callbacks to be called
    # when topologies change.
    self.topologies_watchers = []

    # The dictionary is from the topology name
    # to the callback.
    self.topology_watchers = defaultdict(lambda: [])
    self.execution_state_watchers = defaultdict(lambda: [])
    self.pplan_watchers = defaultdict(lambda: [])
    self.tmaster_watchers = defaultdict(lambda: [])

    # Instantiate the monitoring thread.
    self.monitoring_thread = threading.Thread(target=self.monitor)

  def start(self):
    self.monitoring_thread_stop_signal = False
    self.monitoring_thread.start()

  def stop(self):
    self.monitoring_thread_stop_signal = True

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
      for topology, callbacks in watchers.iteritems():
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
      topologies = filter(lambda f: os.path.isfile(os.path.join(topologies_path, f)), os.listdir(topologies_path))
      if set(topologies) != set(self.topologies_directory):
        for callback in self.topologies_watchers:
          callback(topologies)
      self.topologies_directory = topologies

      trigger_watches_based_on_files(self.topology_watchers, topologies_path, self.topologies_directory, Topology)

      # Get the directory name for execution state
      execution_state_path = os.path.dirname(self.get_execution_state_path(""))
      trigger_watches_based_on_files(self.execution_state_watchers, execution_state_path, self.execution_state_directory, ExecutionState)

      # Get the directory name for pplan
      pplan_path = os.path.dirname(self.get_pplan_path(""))
      trigger_watches_based_on_files(self.pplan_watchers, pplan_path, self.pplan_directory, PhysicalPlan)

      # Get the directory name for tmaster
      tmaster_path = os.path.dirname(self.get_tmaster_path(""))
      trigger_watches_based_on_files(self.tmaster_watchers, tmaster_path, self.tmaster_directory, TMasterLocation)

      # Sleep for some time
      time.sleep(5)

  def get_topologies(self, callback=None):
    if callback:
      self.topologies_watchers.append(callback)
    else:
      topologies_path = self.get_topologies_path()
      return filter(lambda f: os.path.isfile(os.path.join(topologies_path, f)), os.listdir(topologies_path))

  def get_topology(self, topologyName, callback=None):
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

  def get_pplan(self, topologyName, callback=None):
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
    if callback:
      self.tmaster_watchers[topologyName].append(callback)
    else:
      tmaster_path = self.get_tmaster_path(topologyName)
      with open(tmaster_path) as f:
        data = f.read()
        tmaster = TMasterLocation()
        tmaster.ParseFromString(data)
        return tmaster

