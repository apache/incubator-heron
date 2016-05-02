# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging

from functools import partial

from heron.proto import topology_pb2
from heron.statemgrs.src.python import statemanagerfactory
from heron.tracker.src.python.topology import Topology
from heron.tracker.src.python import javaobj
from heron.tracker.src.python import utils

LOG = logging.getLogger(__name__)

class Tracker:
  """
  Tracker is a stateless cache of all the topologies
  for the given state managers. It watches for
  any changes on the topologies, like submission,
  killing, movement of containers, etc..

  This class caches all the data and is accessed
  by handlers.
  """

  def __init__(self, config):
    self.config = config
    self.topologies = []
    self.state_managers = []

    # A map from a tuple of form
    # (topologyName, state_manager_name) to its
    # info, which is its representation
    # exposed through the APIs.
    # The state_manager_name help when we
    # want to remove the topology,
    # since other info can not be relied upon.
    self.topologyInfos = {}

  def synch_topologies(self):
    """
    Sync the topologies with the statemgrs.
    """
    self.state_managers = statemanagerfactory.get_all_state_managers(self.config.statemgr_config)

    def on_topologies_watch(state_manager, topologies):
      LOG.info("State watch triggered for topologies.")
      LOG.debug("Topologies: " + str(topologies))
      existingTopologies = self.getTopologiesForStateLocation(state_manager.name)
      existingTopNames = map(lambda t: t.name, existingTopologies)
      LOG.debug("Existing topologies: " + str(existingTopNames))
      for name in existingTopNames:
        if name not in topologies:
          LOG.info("Removing topology: {0} in rootpath: {1}".format(
            name, state_manager.rootpath))
          self.removeTopology(name, state_manager.name)

      for name in topologies:
        if name not in existingTopNames:
          self.addNewTopology(state_manager, name)

    for state_manager in self.state_managers:
      # The callback function with the bound
      # state_manager as first variable.
      onTopologiesWatch = partial(on_topologies_watch, state_manager)
      state_manager.get_topologies(onTopologiesWatch)

  def getTopologyByClusterEnvironAndName(self, cluster, environ, topName):
    """
    Find and return the topology given its cluster, environ and topology name.
    Raises exception if topology is not found, or more than one are found.
    """
    topologies = filter(lambda t: t.name == topName
                        and t.cluster == cluster
                        and t.environ == environ, self.topologies)
    if not topologies or len(topologies) > 1:
      raise Exception("Topology not found for {0}, {1}, {2}".format(cluster, environ, topName))

    # There is only one topology which is returned.
    return topologies[0]

  def getTopologiesForStateLocation(self, name):
    """
    Returns all the topologies for a given state manager.
    """
    return filter(lambda t: t.state_manager_name == name, self.topologies)

  def addNewTopology(self, state_manager, topologyName):
    """
    Adds a topology in the local cache, and sets a watch
    on any changes on the topology.
    """
    topology = Topology(topologyName, state_manager.name)
    LOG.info("Adding new topology: {0}, state_manager: {1}".format(
      topologyName, state_manager.name))
    self.topologies.append(topology)

    # Register a watch on topology and change
    # the topologyInfo on any new change.
    topology.register_watch(self.setTopologyInfo)

    def on_topology_pplan(data):
      LOG.info("Watch triggered for topology pplan: " + topologyName)
      topology.set_physical_plan(data)
      if not data:
        LOG.debug("No data to be set")

    def on_topology_execution_state(data):
      LOG.info("Watch triggered for topology execution state: " + topologyName)
      topology.set_execution_state(data)
      if not data:
        LOG.debug("No data to be set")

    def on_topology_tmaster(data):
      LOG.info("Watch triggered for topology tmaster: " + topologyName)
      topology.set_tmaster(data)
      if not data:
        LOG.debug("No data to be set")

    # Set watches on the pplan, execution_state, and tmaster.
    state_manager.get_pplan(topologyName, on_topology_pplan)
    state_manager.get_execution_state(topologyName, on_topology_execution_state)
    state_manager.get_tmaster(topologyName, on_topology_tmaster)

  def removeTopology(self, topology_name, state_manager_name):
    """
    Removes the topology from the local cache.
    """
    topologies = []
    for top in self.topologies:
      if (top.name == topology_name and
          top.state_manager_name == state_manager_name):
        # Remove topologyInfo
        if (topology_name, state_manager_name) in self.topologyInfos:
          self.topologyInfos.pop((topology_name, state_manager_name))
      else:
        topologies.append(top)

    self.topologies = topologies

  def extract_execution_state(self, topology):
    """
    Returns the repesentation of execution state that will
    be returned from Tracker.
    """
    execution_state = topology.execution_state

    executionState = {
      "cluster": execution_state.cluster,
      "environ": execution_state.environ,
      "role": execution_state.role,
      "jobname": topology.name,
      "submission_time": execution_state.submission_time,
      "submission_user": execution_state.submission_user,
      "release_username": execution_state.release_state.release_username,
      "release_tag": execution_state.release_state.release_tag,
      "release_version": execution_state.release_state.release_version,
      "uploader_version": execution_state.release_state.uploader_version,
      "has_physical_plan": None,
      "has_tmaster_location": None,
    }

    viz_url = self.config.get_formatted_viz_url(executionState)
    executionState["viz"] = viz_url
    return executionState

  def extract_tmaster(self, topology):
    """
    Returns the representation of tmaster that will
    be returned from Tracker.
    """
    tmasterLocation = {
      "name": None,
      "id": None,
      "host": None,
      "controller_port": None,
      "master_port": None,
      "stats_port": None,
    }
    if topology.tmaster:
      tmasterLocation["name"] = topology.tmaster.topology_name
      tmasterLocation["id"] = topology.tmaster.topology_id
      tmasterLocation["host"] = topology.tmaster.host
      tmasterLocation["controller_port"] = topology.tmaster.controller_port
      tmasterLocation["master_port"] = topology.tmaster.master_port
      tmasterLocation["stats_port"] = topology.tmaster.stats_port

    return tmasterLocation

  def extract_logical_plan(self, topology):
    """
    Returns the representation of logical plan that will
    be returned from Tracker.
    """
    logicalPlan = {
      "spouts": {},
      "bolts": {},
    }

    # Add spouts.
    for spout in topology.spouts():
      spoutName = spout.comp.name
      spoutType = "default"
      spoutSource = "NA"
      spoutVersion = "NA"
      spoutConfigs = spout.comp.config.kvs
      for kvs in spoutConfigs:
        if kvs.key == "spout.type":
          spoutType = kvs.value
        elif kvs.key == "spout.source":
          spoutSource = kvs.value
        elif kvs.key == "spout.version":
          spoutVersion = kvs.value
      spoutPlan = {
        "type": spoutType,
        "source": spoutSource,
        "version": spoutVersion,
        "outputs": []
      }
      for outputStream in list(spout.outputs):
        spoutPlan["outputs"].append({
          "stream_name": outputStream.stream.id
        })

      logicalPlan["spouts"][spoutName] = spoutPlan

    # Add bolts.
    for bolt in topology.bolts():
      boltName = bolt.comp.name
      boltPlan = {
        "outputs": [],
        "inputs": []
      }
      for outputStream in list(bolt.outputs):
        boltPlan["outputs"].append({
          "stream_name": outputStream.stream.id
        })
      for inputStream in list(bolt.inputs):
        boltPlan["inputs"].append({
          "stream_name": inputStream.stream.id,
          "component_name": inputStream.stream.component_name,
          "grouping": topology_pb2.Grouping.Name(inputStream.gtype)
        })

      logicalPlan["bolts"][boltName] = boltPlan

    return logicalPlan

  def extract_physical_plan(self, topology):
    """
    Returns the representation of physical plan that will
    be returned from Tracker.
    """
    physicalPlan = {
      "instances": {},
      "stmgrs": {},
      "spouts": {},
      "bolts": {},
      "config": {},
    }

    if not topology.physical_plan:
      return physicalPlan

    spouts = topology.spouts()
    bolts = topology.bolts()
    stmgrs = None
    instances = None
    configs = {}

    # Physical Plan
    stmgrs = list(topology.physical_plan.stmgrs)
    instances = list(topology.physical_plan.instances)

    # Configs
    if topology.physical_plan.topology.topology_config:
      for kvs in topology.physical_plan.topology.topology_config.kvs:
        if kvs.value:
          physicalPlan["config"][kvs.key] = kvs.value
        elif kvs.java_serialized_value:
          # Hexadecimal byte array for Serialized objects
          try:
            pobj = javaobj.loads(kvs.java_serialized_value)
            physicalPlan["config"][kvs.key] = {
              'value' : json.dumps(pobj,
                                   default=lambda custom_field: custom_field.__dict__,
                                   sort_keys=True,
                                   indent=2),
              'raw' : utils.hex_escape(kvs.java_serialized_value)}
          except Exception as e:
            physicalPlan["config"][kvs.key] = {
              'value' : 'A Java Object',
              'raw' : utils.hex_escape(kvs.java_serialized_value)}
    for spout in spouts:
      spout_name = spout.comp.name
      physicalPlan["spouts"][spout_name] = []
    for bolt in bolts:
      bolt_name = bolt.comp.name
      physicalPlan["bolts"][bolt_name] = []

    for stmgr in stmgrs:
      host = stmgr.host_name
      cwd = stmgr.cwd
      shell_port = stmgr.shell_port if stmgr.HasField("shell_port") else None
      physicalPlan["stmgrs"][stmgr.id] = {
        "id": stmgr.id,
        "host": host,
        "port": stmgr.data_port,
        "shell_port": shell_port,
        "cwd": cwd,
        "pid": stmgr.pid,
        "joburl": utils.make_shell_job_url(host, shell_port, cwd),
        "logfiles": utils.make_shell_logfiles_url(host, shell_port, cwd),
        "instance_ids": []
      }

    for instance in instances:
      taskId = str(instance.info.task_id)
      instance_id = instance.instance_id
      stmgrId = instance.stmgr_id
      name = instance.info.component_name
      stmgrInfo = physicalPlan["stmgrs"][stmgrId]
      host = stmgrInfo["host"]
      cwd = stmgrInfo["cwd"]
      shell_port = stmgrInfo["shell_port"]

      physicalPlan["instances"][instance_id] = {
        "id": instance_id,
        "name": name,
        "stmgrId": stmgrId,
        "logfile": utils.make_shell_logfiles_url(host, shell_port, cwd, instance.instance_id),
      }
      physicalPlan["stmgrs"][stmgrId]["instance_ids"].append(instance_id)
      if name in physicalPlan["spouts"]:
        physicalPlan["spouts"][name].append(instance_id)
      else:
        physicalPlan["bolts"][name].append(instance_id)

    return physicalPlan

  def setTopologyInfo(self, topology):
    """
    Extracts info from the stored proto states and
    convert it into representation that is exposed using
    the API.
    This method is called on any change for the topology.
    For example, when a container moves and its host or some
    port changes. All the information is parsed all over
    again and cache is updated.
    """
    # Execution state is the most basic info.
    # If there is no execution state, just return
    # as the rest of the things don't matter.
    if not topology.execution_state:
      LOG.info("No execution state found for: " + topology.name)
      return

    LOG.info("Setting topology info for topology: " + topology.name)
    has_physical_plan = True
    if not topology.physical_plan:
      has_physical_plan = False

    has_tmaster_location = True
    if not topology.tmaster:
      has_tmaster_location = False

    top = {
      "name": topology.name,
      "id": topology.id,
      "logical_plan": None,
      "physical_plan": None,
      "execution_state": None,
      "tmaster_location": None,
    }

    executionState = self.extract_execution_state(topology)
    executionState["has_physical_plan"] = has_physical_plan
    executionState["has_tmaster_location"] = has_tmaster_location

    top["execution_state"] = executionState
    top["logical_plan"] = self.extract_logical_plan(topology)
    top["physical_plan"] = self.extract_physical_plan(topology)
    top["tmaster_location"] = self.extract_tmaster(topology)

    self.topologyInfos[(topology.name, topology.state_manager_name)] = top

  def getTopologyInfo(self, topologyName, cluster, environ):
    """
    Returns the JSON representation of a topology
    by its name, cluster and environ.
    Raises exception if no such topology is found.
    """
    # Iterate over the values to filter the desired topology.
    for (topology_name, state_manager_name), topologyInfo in self.topologyInfos.iteritems():
      executionState = topologyInfo["execution_state"]
      if (topologyName == topology_name and
          cluster == executionState["cluster"] and
          environ == executionState["environ"]):
        return topologyInfo
    LOG.info("Could not find topology info for topology: {0}, \
             cluster: {1} and environ: {2}".format(topologyName, cluster, environ))
    raise Exception("No topology found")

