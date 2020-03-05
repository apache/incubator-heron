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

''' tracker.py '''
import json
import sys
import traceback
import collections

from functools import partial

from heron.common.src.python.utils.log import Log
from heron.proto import topology_pb2
from heron.statemgrs.src.python import statemanagerfactory
from heron.tools.tracker.src.python.config import EXTRA_LINK_FORMATTER_KEY, EXTRA_LINK_URL_KEY
from heron.tools.tracker.src.python.topology import Topology
from heron.tools.tracker.src.python import javaobj
from heron.tools.tracker.src.python import pyutils
from heron.tools.tracker.src.python import utils


def convert_pb_kvs(kvs, include_non_primitives=True):
  """
  converts pb kvs to dict
  """
  config = {}
  for kv in kvs:
    if kv.value:
      config[kv.key] = kv.value
    elif kv.serialized_value:
      # add serialized_value support for python values (fixme)

      # is this a serialized java object
      if topology_pb2.JAVA_SERIALIZED_VALUE == kv.type:
        jv = _convert_java_value(kv, include_non_primitives=include_non_primitives)
        if jv is not None:
          config[kv.key] = jv
      else:
        config[kv.key] = _raw_value(kv)
  return config


def _convert_java_value(kv, include_non_primitives=True):
  try:
    pobj = javaobj.loads(kv.serialized_value)
    if pyutils.is_str_instance(pobj):
      return pobj

    if pobj.is_primitive():
      return pobj.value

    if include_non_primitives:
      # java objects that are not strings return value and encoded value
      # Hexadecimal byte array for Serialized objects that
      return {
          'value' : json.dumps(pobj,
                               default=lambda custom_field: custom_field.__dict__,
                               sort_keys=True,
                               indent=2),
          'raw' : utils.hex_escape(kv.serialized_value)}

    return None
  except Exception:
    Log.exception("Failed to parse data as java object")
    if include_non_primitives:
      return _raw_value(kv)
    else:
      return None

def _raw_value(kv):
  return {
      # The value should be a valid json object
      'value' : '{}',
      'raw' : utils.hex_escape(kv.serialized_value)}


class Tracker(object):
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
    try:
      for state_manager in self.state_managers:
        state_manager.start()
    except Exception as ex:
      Log.error("Found exception while initializing state managers: %s. Bailing out..." % ex)
      traceback.print_exc()
      sys.exit(1)

    # pylint: disable=deprecated-lambda
    def on_topologies_watch(state_manager, topologies):
      """watch topologies"""
      Log.info("State watch triggered for topologies.")
      Log.debug("Topologies: " + str(topologies))
      existingTopologies = self.getTopologiesForStateLocation(state_manager.name)
      existingTopNames = [t.name for t in existingTopologies]
      Log.debug("Existing topologies: " + str(existingTopNames))
      for name in existingTopNames:
        if name not in topologies:
          Log.info("Removing topology: %s in rootpath: %s",
                   name, state_manager.rootpath)
          self.removeTopology(name, state_manager.name)

      for name in topologies:
        if name not in existingTopNames:
          self.addNewTopology(state_manager, name)

    for state_manager in self.state_managers:
      # The callback function with the bound
      # state_manager as first variable.
      onTopologiesWatch = partial(on_topologies_watch, state_manager)
      state_manager.get_topologies(onTopologiesWatch)

  def stop_sync(self):
    for state_manager in self.state_managers:
      state_manager.stop()

  # pylint: disable=deprecated-lambda
  def getTopologyByClusterRoleEnvironAndName(self, cluster, role, environ, topologyName):
    """
    Find and return the topology given its cluster, environ, topology name, and
    an optional role.
    Raises exception if topology is not found, or more than one are found.
    """
    topologies = list([t for t in self.topologies if t.name == topologyName
                       and t.cluster == cluster
                       and (not role or t.execution_state.role == role)
                       and t.environ == environ])
    if not topologies or len(topologies) > 1:
      if role is not None:
        raise Exception("Topology not found for {0}, {1}, {2}, {3}".format(
            cluster, role, environ, topologyName))
      else:
        raise Exception("Topology not found for {0}, {1}, {2}".format(
            cluster, environ, topologyName))

    # There is only one topology which is returned.
    return topologies[0]

  def getTopologiesForStateLocation(self, name):
    """
    Returns all the topologies for a given state manager.
    """
    return [t for t in self.topologies if t.state_manager_name == name]

  def addNewTopology(self, state_manager, topologyName):
    """
    Adds a topology in the local cache, and sets a watch
    on any changes on the topology.
    """
    topology = Topology(topologyName, state_manager.name)
    Log.info("Adding new topology: %s, state_manager: %s",
             topologyName, state_manager.name)
    self.topologies.append(topology)

    # Register a watch on topology and change
    # the topologyInfo on any new change.
    topology.register_watch(self.setTopologyInfo)

    def on_topology_pplan(data):
      """watch physical plan"""
      Log.info("Watch triggered for topology pplan: " + topologyName)
      topology.set_physical_plan(data)
      if not data:
        Log.debug("No data to be set")

    def on_topology_packing_plan(data):
      """watch packing plan"""
      Log.info("Watch triggered for topology packing plan: " + topologyName)
      topology.set_packing_plan(data)
      if not data:
        Log.debug("No data to be set")

    def on_topology_execution_state(data):
      """watch execution state"""
      Log.info("Watch triggered for topology execution state: " + topologyName)
      topology.set_execution_state(data)
      if not data:
        Log.debug("No data to be set")

    def on_topology_tmaster(data):
      """set tmaster"""
      Log.info("Watch triggered for topology tmaster: " + topologyName)
      topology.set_tmaster(data)
      if not data:
        Log.debug("No data to be set")

    def on_topology_scheduler_location(data):
      """set scheduler location"""
      Log.info("Watch triggered for topology scheduler location: " + topologyName)
      topology.set_scheduler_location(data)
      if not data:
        Log.debug("No data to be set")

    # Set watches on the pplan, execution_state, tmaster and scheduler_location.
    state_manager.get_pplan(topologyName, on_topology_pplan)
    state_manager.get_packing_plan(topologyName, on_topology_packing_plan)
    state_manager.get_execution_state(topologyName, on_topology_execution_state)
    state_manager.get_tmaster(topologyName, on_topology_tmaster)
    state_manager.get_scheduler_location(topologyName, on_topology_scheduler_location)

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
        "has_physical_plan": None,
        "has_tmaster_location": None,
        "has_scheduler_location": None,
        "extra_links": [],
    }

    for extra_link in self.config.extra_links:
      link = extra_link.copy()
      link[EXTRA_LINK_URL_KEY] = self.config.get_formatted_url(link[EXTRA_LINK_FORMATTER_KEY],
                                                               executionState)
      executionState["extra_links"].append(link)
    return executionState

  def extract_metadata(self, topology):
    """
    Returns metadata that will
    be returned from Tracker.
    """
    execution_state = topology.execution_state
    metadata = {
        "cluster": execution_state.cluster,
        "environ": execution_state.environ,
        "role": execution_state.role,
        "jobname": topology.name,
        "submission_time": execution_state.submission_time,
        "submission_user": execution_state.submission_user,
        "release_username": execution_state.release_state.release_username,
        "release_tag": execution_state.release_state.release_tag,
        "release_version": execution_state.release_state.release_version,
        "extra_links": [],
    }

    for extra_link in self.config.extra_links:
      link = extra_link.copy()
      link[EXTRA_LINK_URL_KEY] = self.config.get_formatted_url(link[EXTRA_LINK_FORMATTER_KEY],
                                                               metadata)
      metadata["extra_links"].append(link)
    return metadata

  @staticmethod
  def extract_runtime_state(topology):
    runtime_state = {}
    runtime_state["has_physical_plan"] = \
      True if topology.physical_plan else False
    runtime_state["has_packing_plan"] = \
      True if topology.packing_plan else False
    runtime_state["has_tmaster_location"] = \
      True if topology.tmaster else False
    runtime_state["has_scheduler_location"] = \
      True if topology.scheduler_location else False
    # "stmgrs" listed runtime state for each stream manager
    # however it is possible that physical plan is not complete
    # yet and we do not know how many stmgrs there are. That said,
    # we should not set any key below (stream manager name)
    runtime_state["stmgrs"] = {}
    return runtime_state

  # pylint: disable=no-self-use
  def extract_scheduler_location(self, topology):
    """
    Returns the representation of scheduler location that will
    be returned from Tracker.
    """
    schedulerLocation = {
        "name": None,
        "http_endpoint": None,
        "job_page_link": None,
    }

    if topology.scheduler_location:
      schedulerLocation["name"] = topology.scheduler_location.topology_name
      schedulerLocation["http_endpoint"] = topology.scheduler_location.http_endpoint
      schedulerLocation["job_page_link"] = \
          topology.scheduler_location.job_page_link[0] \
          if len(topology.scheduler_location.job_page_link) > 0 else ""

    return schedulerLocation

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

  # pylint: disable=too-many-locals
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
      spoutExtraLinks = []
      for kvs in spoutConfigs:
        if kvs.key == "spout.type":
          spoutType = javaobj.loads(kvs.serialized_value)
        elif kvs.key == "spout.source":
          spoutSource = javaobj.loads(kvs.serialized_value)
        elif kvs.key == "spout.version":
          spoutVersion = javaobj.loads(kvs.serialized_value)
        elif kvs.key == "extra.links":
          spoutExtraLinks = json.loads(javaobj.loads(kvs.serialized_value))

      spoutPlan = {
          "config": convert_pb_kvs(spoutConfigs, include_non_primitives=False),
          "type": spoutType,
          "source": spoutSource,
          "version": spoutVersion,
          "outputs": [],
          "extra_links": spoutExtraLinks,
      }

      # render component extra links with general params
      execution_state = topology.execution_state
      executionState = {
          "cluster": execution_state.cluster,
          "environ": execution_state.environ,
          "role": execution_state.role,
          "jobname": topology.name,
          "submission_user": execution_state.submission_user,
      }

      for link in spoutPlan["extra_links"]:
        link[EXTRA_LINK_URL_KEY] = self.config.get_formatted_url(link[EXTRA_LINK_FORMATTER_KEY],
                                                                 executionState)

      for outputStream in list(spout.outputs):
        spoutPlan["outputs"].append({
            "stream_name": outputStream.stream.id
        })

      logicalPlan["spouts"][spoutName] = spoutPlan

    # Add bolts.
    for bolt in topology.bolts():
      boltName = bolt.comp.name
      boltPlan = {
          "config": convert_pb_kvs(bolt.comp.config.kvs, include_non_primitives=False),
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

  # pylint: disable=too-many-locals
  def extract_physical_plan(self, topology):
    """
    Returns the representation of physical plan that will
    be returned from Tracker.
    """
    physicalPlan = {
        "instances": {},
        "instance_groups": {},
        "stmgrs": {},
        "spouts": {},
        "bolts": {},
        "config": {},
        "components": {}
    }

    if not topology.physical_plan:
      return physicalPlan

    spouts = topology.spouts()
    bolts = topology.bolts()
    stmgrs = None
    instances = None

    # Physical Plan
    stmgrs = list(topology.physical_plan.stmgrs)
    instances = list(topology.physical_plan.instances)

    # Configs
    if topology.physical_plan.topology.topology_config:
      physicalPlan["config"] = convert_pb_kvs(topology.physical_plan.topology.topology_config.kvs)

    for spout in spouts:
      spout_name = spout.comp.name
      physicalPlan["spouts"][spout_name] = []
      if spout_name not in physicalPlan["components"]:
        physicalPlan["components"][spout_name] = {
            "config": convert_pb_kvs(spout.comp.config.kvs)
        }
    for bolt in bolts:
      bolt_name = bolt.comp.name
      physicalPlan["bolts"][bolt_name] = []
      if bolt_name not in physicalPlan["components"]:
        physicalPlan["components"][bolt_name] = {
            "config": convert_pb_kvs(bolt.comp.config.kvs)
        }

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

    instance_groups = collections.OrderedDict()
    for instance in instances:
      instance_id = instance.instance_id
      stmgrId = instance.stmgr_id
      name = instance.info.component_name
      stmgrInfo = physicalPlan["stmgrs"][stmgrId]
      host = stmgrInfo["host"]
      cwd = stmgrInfo["cwd"]
      shell_port = stmgrInfo["shell_port"]


      # instance_id format container_<index>_component_1
      # group name is container_<index>
      group_name = instance_id.rsplit("_", 2)[0]
      igroup = instance_groups.get(group_name, list())
      igroup.append(instance_id)
      instance_groups[group_name] = igroup

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

    physicalPlan["instance_groups"] = instance_groups

    return physicalPlan

  # pylint: disable=too-many-locals
  def extract_packing_plan(self, topology):
    """
    Returns the representation of packing plan that will
    be returned from Tracker.
    """
    packingPlan = {
        "id": "",
        "container_plans": []
    }

    if not topology.packing_plan:
      return packingPlan

    container_plans = topology.packing_plan.container_plans

    containers = []
    for container_plan in container_plans:
      instances = []
      for instance_plan in container_plan.instance_plans:
        instance_resources = {"cpu": instance_plan.resource.cpu,
                              "ram": instance_plan.resource.ram,
                              "disk": instance_plan.resource.disk}
        instance = {"component_name" : instance_plan.component_name,
                    "task_id" : instance_plan.task_id,
                    "component_index": instance_plan.component_index,
                    "instance_resources": instance_resources}
        instances.append(instance)
      required_resource = {"cpu": container_plan.requiredResource.cpu,
                           "ram": container_plan.requiredResource.ram,
                           "disk": container_plan.requiredResource.disk}
      scheduled_resource = {}
      if container_plan.scheduledResource:
        scheduled_resource = {"cpu": container_plan.scheduledResource.cpu,
                              "ram": container_plan.scheduledResource.ram,
                              "disk": container_plan.scheduledResource.disk}
      container = {"id": container_plan.id,
                   "instances": instances,
                   "required_resources": required_resource,
                   "scheduled_resources": scheduled_resource}
      containers.append(container)

    packingPlan["id"] = topology.packing_plan.id
    packingPlan["container_plans"] = containers
    return packingPlan

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
      Log.info("No execution state found for: " + topology.name)
      return

    Log.info("Setting topology info for topology: " + topology.name)
    has_physical_plan = True
    if not topology.physical_plan:
      has_physical_plan = False

    Log.info("Setting topology info for topology: " + topology.name)
    has_packing_plan = True
    if not topology.packing_plan:
      has_packing_plan = False

    has_tmaster_location = True
    if not topology.tmaster:
      has_tmaster_location = False

    has_scheduler_location = True
    if not topology.scheduler_location:
      has_scheduler_location = False

    topologyInfo = {
        "name": topology.name,
        "id": topology.id,
        "logical_plan": None,
        "physical_plan": None,
        "packing_plan": None,
        "execution_state": None,
        "tmaster_location": None,
        "scheduler_location": None,
    }

    executionState = self.extract_execution_state(topology)
    executionState["has_physical_plan"] = has_physical_plan
    executionState["has_packing_plan"] = has_packing_plan
    executionState["has_tmaster_location"] = has_tmaster_location
    executionState["has_scheduler_location"] = has_scheduler_location
    executionState["status"] = topology.get_status()

    topologyInfo["metadata"] = self.extract_metadata(topology)
    topologyInfo["runtime_state"] = self.extract_runtime_state(topology)

    topologyInfo["execution_state"] = executionState
    topologyInfo["logical_plan"] = self.extract_logical_plan(topology)
    topologyInfo["physical_plan"] = self.extract_physical_plan(topology)
    topologyInfo["packing_plan"] = self.extract_packing_plan(topology)
    topologyInfo["tmaster_location"] = self.extract_tmaster(topology)
    topologyInfo["scheduler_location"] = self.extract_scheduler_location(topology)

    self.topologyInfos[(topology.name, topology.state_manager_name)] = topologyInfo

  def getTopologyInfo(self, topologyName, cluster, role, environ):
    """
    Returns the JSON representation of a topology
    by its name, cluster, environ, and an optional role parameter.
    Raises exception if no such topology is found.
    """
    # Iterate over the values to filter the desired topology.
    for (topology_name, _), topologyInfo in list(self.topologyInfos.items()):
      executionState = topologyInfo["execution_state"]
      if (topologyName == topology_name and
          cluster == executionState["cluster"] and
          environ == executionState["environ"]):
        # If role is specified, first try to match "role" field. If "role" field
        # does not exist, try to match "submission_user" field.
        if not role or executionState.get("role") == role:
          return topologyInfo
    if role is not None:
      Log.info("Could not find topology info for topology: %s," \
               "cluster: %s, role: %s, and environ: %s",
               topologyName, cluster, role, environ)
    else:
      Log.info("Could not find topology info for topology: %s," \
               "cluster: %s and environ: %s", topologyName, cluster, environ)
    raise Exception("No topology found")
