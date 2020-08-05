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
import json
import sys
import collections

from functools import partial
from typing import Any, Dict, List, Optional, Tuple

from heron.common.src.python.utils.log import Log
from heron.proto import topology_pb2
from heron.statemgrs.src.python import statemanagerfactory
from heron.tools.tracker.src.python.config import EXTRA_LINK_FORMATTER_KEY, EXTRA_LINK_URL_KEY
from heron.tools.tracker.src.python.topology import Topology
from heron.tools.tracker.src.python import utils

import javaobj.v1 as javaobj

def convert_pb_kvs(kvs, include_non_primitives=True) -> dict:
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
    if isinstance(pobj, str):
      return pobj

    if isinstance(pobj, javaobj.transformers.DefaultObjectTransformer.JavaPrimitiveClass):
      return pobj.value

    if include_non_primitives:
      # java objects that are not strings return value and encoded value
      # Hexadecimal byte array for Serialized objects that
      return {
          'value' : json.dumps(pobj,
                               default=lambda custom_field: custom_field.__dict__,
                               sort_keys=True,
                               indent=2),
          'raw' : kv.serialized_value.hex()}

    return None
  except Exception:
    Log.exception("Failed to parse data as java object")
    if include_non_primitives:
      return _raw_value(kv)
    return None

def _raw_value(kv):
  return {
      # The value should be a valid json object
      'value' : '{}',
      'raw' : kv.serialized_value.hex()}


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
    # (topology_name, state_manager_name) to its
    # info, which is its representation
    # exposed through the APIs.
    # The state_manager_name help when we
    # want to remove the topology,
    # since other info can not be relied upon.
    self.topology_infos: Dict[Tuple[str, str], Any] = {}

  def synch_topologies(self) -> None:
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

    def on_topologies_watch(state_manager, topologies) -> None:
      """watch topologies"""
      Log.info("State watch triggered for topologies.")
      Log.debug("Topologies: " + str(topologies))
      cached_names = [t.name for t in self.get_stmgr_topologies(state_manager.name)]
      Log.debug(f"Existing topologies: {cached_names}")
      for name in cached_names:
        if name not in topologies:
          Log.info("Removing topology: %s in rootpath: %s",
                   name, state_manager.rootpath)
          self.remove_topology(name, state_manager.name)

      for name in topologies:
        if name not in cached_names:
          self.add_new_topology(state_manager, name)

    for state_manager in self.state_managers:
      # The callback function with the bound
      # state_manager as first variable.
      onTopologiesWatch = partial(on_topologies_watch, state_manager)
      state_manager.get_topologies(onTopologiesWatch)

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
        raise Exception("Topology not found for {0}, {1}, {2}, {3}".format(
            cluster, role, environ, topology_name))
      raise Exception("Topology not found for {0}, {1}, {2}".format(
          cluster, environ, topology_name))

    # There is only one topology which is returned.
    return topologies[0]

  def get_stmgr_topologies(self, name: str) -> List[Any]:
    """
    Returns all the topologies for a given state manager.
    """
    return [t for t in self.topologies if t.state_manager_name == name]

  def add_new_topology(self, state_manager, topology_name: str) -> None:
    """
    Adds a topology in the local cache, and sets a watch
    on any changes on the topology.
    """
    topology = Topology(topology_name, state_manager.name)
    Log.info("Adding new topology: %s, state_manager: %s",
             topology_name, state_manager.name)
    self.topologies.append(topology)

    # Register a watch on topology and change
    # the topology_info on any new change.
    topology.register_watch(self.set_topology_info)

    # Set watches on the pplan, execution_state, tmaster and scheduler_location.
    state_manager.get_pplan(topology_name, topology.set_physical_plan)
    state_manager.get_packing_plan(topology_name, topology.set_packing_plan)
    state_manager.get_execution_state(topology_name, topology.set_execution_state)
    state_manager.get_tmaster(topology_name, topology.set_tmaster)
    state_manager.get_scheduler_location(topology_name, topology.set_scheduler_location)

  def remove_topology(self, topology_name: str, state_manager_name: str) -> None:
    """
    Removes the topology from the local cache.
    """
    topologies = []
    for top in self.topologies:
      if (top.name == topology_name and
          top.state_manager_name == state_manager_name):
        # Remove topology_info
        if (topology_name, state_manager_name) in self.topology_infos:
          self.topology_infos.pop((topology_name, state_manager_name))
      else:
        topologies.append(top)

    self.topologies = topologies

  def extract_execution_state(self, topology) -> dict:
    """
    Returns the repesentation of execution state that will
    be returned from Tracker.

    It looks like this has been replaced with extract_metadata and
    extract_runtime_state.

    """
    result = self.extract_metadata(topology)
    result.update({
        "has_physical_plan": None,
        "has_tmaster_location": None,
        "has_scheduler_location": None,
    })
    return result

  def extract_metadata(self, topology) -> dict:
    """
    Returns metadata that will be returned from Tracker.
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
    # "stmgrs" listed runtime state for each stream manager
    # however it is possible that physical plan is not complete
    # yet and we do not know how many stmgrs there are. That said,
    # we should not set any key below (stream manager name)
    return {
        "has_physical_plan":  bool(topology.physical_plan),
        "has_packing_plan":  bool(topology.packing_plan),
        "has_tmaster_location":  bool(topology.tmaster),
        "has_scheduler_location":  bool(topology.scheduler_location),
        "stmgrs": {},
    }

  # pylint: disable=no-self-use
  def extract_scheduler_location(self, topology) -> dict:
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
          if topology.scheduler_location.job_page_link else ""

    return schedulerLocation

  def extract_tmaster(self, topology) -> dict:
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
          "outputs": [
              {"stream_name": outputStream.stream.id}
              for outputStream in spout.outputs
          ],
          "extra_links": spoutExtraLinks,
      }
      logicalPlan["spouts"][spout.comp.name] = spoutPlan

      # render component extra links with general params
      execution_state = {
          "cluster": topology.execution_state.cluster,
          "environ": topology.execution_state.environ,
          "role": topology.execution_state.role,
          "jobname": topology.name,
          "submission_user": topology.execution_state.submission_user,
      }

      for link in spoutPlan["extra_links"]:
        link[EXTRA_LINK_URL_KEY] = self.config.get_formatted_url(link[EXTRA_LINK_FORMATTER_KEY],
                                                                 execution_state)

    # Add bolts.
    for bolt in topology.bolts():
      boltName = bolt.comp.name
      logicalPlan["bolts"][boltName] = {
          "config": convert_pb_kvs(bolt.comp.config.kvs, include_non_primitives=False),
          "outputs": [
              {"stream_name": outputStream.stream.id}
              for outputStream in bolt.outputs
          ],
          "inputs": [
              {
                  "stream_name": inputStream.stream.id,
                  "component_name": inputStream.stream.component_name,
                  "grouping": topology_pb2.Grouping.Name(inputStream.gtype),
              }
              for inputStream in bolt.inputs
          ]
      }


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
    Returns the representation of packing plan that will be returned from Tracker.

    """
    packingPlan = {
        "id": "",
        "container_plans": []
    }

    if not topology.packing_plan:
      return packingPlan

    packingPlan["id"] = topology.packing_plan.id
    packingPlan["container_plans"] = [
        {
            "id": container_plan.id,
            "instances": [
                {
                    "component_name" : instance_plan.component_name,
                    "task_id" : instance_plan.task_id,
                    "component_index": instance_plan.component_index,
                    "instance_resources": {
                        "cpu": instance_plan.resource.cpu,
                        "ram": instance_plan.resource.ram,
                        "disk": instance_plan.resource.disk,
                    },
                }
                for instance_plan in container_plan.instance_plans
            ],
            "required_resources": {
                "cpu": container_plan.requiredResource.cpu,
                "ram": container_plan.requiredResource.ram,
                "disk": container_plan.requiredResource.disk,
            },
            "scheduled_resources": (
                {}
                if not container_plan else
                {
                    "cpu": container_plan.scheduledResource.cpu,
                    "ram": container_plan.scheduledResource.ram,
                    "disk": container_plan.scheduledResource.disk,
                }
            ),
        }
        for container_plan in topology.packing_plan.container_plans
    ]

    return packingPlan

  def set_topology_info(self, topology) -> Optional[dict]:
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

    topology_info = {
        "name": topology.name,
        "id": topology.id,
        "logical_plan": None,
        "physical_plan": None,
        "packing_plan": None,
        "execution_state": None,
        "tmaster_location": None,
        "scheduler_location": None,
    }

    execution_state = self.extract_execution_state(topology)
    execution_state["has_physical_plan"] = has_physical_plan
    execution_state["has_packing_plan"] = has_packing_plan
    execution_state["has_tmaster_location"] = has_tmaster_location
    execution_state["has_scheduler_location"] = has_scheduler_location
    execution_state["status"] = topology.get_status()

    topology_info["metadata"] = self.extract_metadata(topology)
    topology_info["runtime_state"] = self.extract_runtime_state(topology)

    topology_info["execution_state"] = execution_state
    topology_info["logical_plan"] = self.extract_logical_plan(topology)
    topology_info["physical_plan"] = self.extract_physical_plan(topology)
    topology_info["packing_plan"] = self.extract_packing_plan(topology)
    topology_info["tmaster_location"] = self.extract_tmaster(topology)
    topology_info["scheduler_location"] = self.extract_scheduler_location(topology)

    self.topology_infos[(topology.name, topology.state_manager_name)] = topology_info

  # topology_name should be at the end to follow the trend
  def get_topology_info(
      self,
      topology_name: str,
      cluster: str,
      role: Optional[str],
      environ: str,
  ) -> str:
    """
    Returns the JSON representation of a topology
    by its name, cluster, environ, and an optional role parameter.
    Raises exception if no such topology is found.

    """
    # Iterate over the values to filter the desired topology.
    for (tn, _), topology_info in self.topology_infos.items():
      execution_state = topology_info["execution_state"]
      if (tn == topology_name and
          cluster == execution_state["cluster"] and
          environ == execution_state["environ"] and
          (not role or execution_state.get("role") == role)
         ):
        return topology_info

    Log.info(
        f"Count not find topology info for cluster={cluster!r},"
        f" role={role!r}, environ={environ!r}, role={role!r},"
        f" topology={topology_name!r}"
    )
    raise Exception("No topology found")
