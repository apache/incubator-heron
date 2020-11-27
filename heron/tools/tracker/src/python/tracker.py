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
from typing import Any, Dict, List, Optional
from weakref import WeakKeyDictionary

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
    self._pb2_to_api_cache = WeakKeyDictionary()

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
      Log.debug("Topologies: %s" + topologies)
      cached_names = [t.name for t in self.get_stmgr_topologies(state_manager.name)]
      Log.debug("Existing topologies: %s", cached_names)
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
        raise KeyError("Topology not found for {0}, {1}, {2}, {3}".format(
            cluster, role, environ, topology_name))
      raise KeyError("Topology not found for {0}, {1}, {2}".format(
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
    # populate the cache before making it addressable in the topologies to
    # avoid races due to concurrent execution
    info = self._pb2_to_api(topology)
    if info is not None:
      self._pb2_to_api_cache[topology] = info
    self.topologies.append(topology)

    topology.register_watch(self.pb2_to_api)

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
    self.topologies = [
        topology
        for topology in self.topologies
        if not (topology.name == topology_name and topology.state_manager_name == state_manager_name)
    ]

  def _pb2_to_api(self, topology: topology_pb2.Topology) -> Optional[Dict[str, Any]]:
    """
    Extracts info from the stored proto states and convert it into representation
    that is exposed using the API.
    """
    # Execution state is the most basic info.
    # If there is no execution state, just return
    # as the rest of the things don't matter.
    if not topology.execution_state:
      Log.info("No execution state found for: " + topology.name)
      return None

    info = {
      "execution_state": self.extract_execution_state(topology),
      "id": topology.id,
      "logical_plan": self.extract_logical_plan(topology),
      "metadata": self.extract_metadata(topology),
      "name": topology.name,
      "packing_plan": self.extract_packing_plan(topology),
      "physical_plan": self.extract_physical_plan(topology),
      "runtime_state": self.extract_runtime_state(topology),
      "scheduler_location": self.extract_scheduler_location(topology),
      "tmanager_location": self.extract_tmanager(topology),
    }

    return info

  def extract_execution_state(self, topology) -> dict:
    """
    Returns the repesentation of execution state that will
    be returned from Tracker.

    It looks like this has been replaced with extract_metadata and
    extract_runtime_state.

    """
    result = self.extract_metadata(topology)
    result.update({
      "has_physical_plan": bool(topology.physical_plan),
      "has_packing_plan": bool(topology.has_packing_plan),
      "has_tmanager_location": bool(topology.tmanager),
      "has_scheduler_location": bool(topology.scheduler_location),
      "status": topology.get_status(),
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
        "has_tmanager_location":  bool(topology.tmanager),
        "has_scheduler_location":  bool(topology.scheduler_location),
        "stmgrs": {},
    }

  # pylint: disable=no-self-use
  def extract_scheduler_location(self, topology) -> dict:
    """
    Returns the representation of scheduler location that will
    be returned from Tracker.
    """
    if topology.scheduler_location:
      return {
          "name": topology.scheduler_location.topology_name,
          "http_endpoint": topology.scheduler_location.http_endpoint,
          "job_page_link": (
              topology.scheduler_location.job_page_link[0] \
              if topology.scheduler_location.job_page_link
              else ""
          ),
      }
      return {
          "name": None,
          "http_endpoint": None,
          "job_page_link": None,
      }

  def extract_tmanager(self, topology) -> dict:
    """
    Returns the representation of tmanager that will
    be returned from Tracker.
    """
    if topology.tmanager:
      return {
          "name": topology.tmanager.topology_name,
          "id": topology.tmanager.topology_id,
          "host": topology.tmanager.host,
          "controller_port": topology.tmanager.controller_port,
          "server_port": topology.tmanager.server_port,
          "stats_port": topology.tmanager.stats_port,
      }
    return {
        "name": None,
        "id": None,
        "host": None,
        "controller_port": None,
        "server_port": None,
        "stats_port": None,
    }

  # pylint: disable=too-many-locals
  def extract_logical_plan(self, topology):
    """
    Returns the representation of logical plan that will
    be returned from Tracker.
    """
    logical_plan = {
        "spouts": {},
        "bolts": {},
    }

    # Add spouts.
    for spout in topology.spouts():
      spout_type = "default"
      spout_source = "NA"
      spout_version = "NA"
      spout_config = spout.comp.config.kvs
      spout_extra_links = []
      for kvs in spout_config:
        if kvs.key == "spout.type":
          spout_type = javaobj.loads(kvs.serialized_value)
        elif kvs.key == "spout.source":
          spout_source = javaobj.loads(kvs.serialized_value)
        elif kvs.key == "spout.version":
          spout_version = javaobj.loads(kvs.serialized_value)
        elif kvs.key == "extra.links":
          spout_extra_links = json.loads(javaobj.loads(kvs.serialized_value))

      spout_plan = {
          "config": convert_pb_kvs(spout_config, include_non_primitives=False),
          "type": spout_type,
          "source": spout_source,
          "version": spout_version,
          "outputs": [
              {"stream_name": output.stream.id}
              for output in spout.outputs
          ],
          "extra_links": spout_extra_links,
      }
      logical_plan["spouts"][spout.comp.name] = spout_plan

      # render component extra links with general params
      execution_state = {
          "cluster": topology.execution_state.cluster,
          "environ": topology.execution_state.environ,
          "role": topology.execution_state.role,
          "jobname": topology.name,
          "submission_user": topology.execution_state.submission_user,
      }

      for link in spout_plan["extra_links"]:
        link[EXTRA_LINK_URL_KEY] = self.config.get_formatted_url(link[EXTRA_LINK_FORMATTER_KEY],
                                                                 execution_state)

    # Add bolts.
    for bolt in topology.bolts():
      bolt_name = bolt.comp.name
      logical_plan["bolts"][bolt_name] = {
          "config": convert_pb_kvs(bolt.comp.config.kvs, include_non_primitives=False),
          "outputs": [
              {"stream_name": output.stream.id}
              for output in bolt.outputs
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


    return logical_plan

  # pylint: disable=too-many-locals
  def extract_physical_plan(self, topology):
    """
    Returns the representation of physical plan that will
    be returned from Tracker.
    """
    physical_plan = {
        "instances": {},
        "instance_groups": {},
        "stmgrs": {},
        "spouts": {},
        "bolts": {},
        "config": {},
        "components": {}
    }

    if not topology.physical_plan:
      return physical_plan

    spouts = topology.spouts()
    bolts = topology.bolts()
    stmgrs = None
    instances = None

    # Physical Plan
    stmgrs = list(topology.physical_plan.stmgrs)
    instances = list(topology.physical_plan.instances)

    # Configs
    if topology.physical_plan.topology.topology_config:
      physical_plan["config"] = convert_pb_kvs(topology.physical_plan.topology.topology_config.kvs)

    for spout in spouts:
      spout_name = spout.comp.name
      physical_plan["spouts"][spout_name] = []
      if spout_name not in physical_plan["components"]:
        physical_plan["components"][spout_name] = {
            "config": convert_pb_kvs(spout.comp.config.kvs)
        }
    for bolt in bolts:
      bolt_name = bolt.comp.name
      physical_plan["bolts"][bolt_name] = []
      if bolt_name not in physical_plan["components"]:
        physical_plan["components"][bolt_name] = {
            "config": convert_pb_kvs(bolt.comp.config.kvs)
        }

    for stmgr in stmgrs:
      host = stmgr.host_name
      cwd = stmgr.cwd
      shell_port = stmgr.shell_port if stmgr.HasField("shell_port") else None
      physical_plan["stmgrs"][stmgr.id] = {
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
      stmgr_id = instance.stmgr_id
      name = instance.info.component_name
      stmgr_info = physical_plan["stmgrs"][stmgr_id]
      host = stmgr_info["host"]
      cwd = stmgr_info["cwd"]
      shell_port = stmgr_info["shell_port"]


      # instance_id format container_<index>_component_1
      # group name is container_<index>
      group_name = instance_id.rsplit("_", 2)[0]
      igroup = instance_groups.get(group_name, list())
      igroup.append(instance_id)
      instance_groups[group_name] = igroup

      physical_plan["instances"][instance_id] = {
          "id": instance_id,
          "name": name,
          "stmgr_id": stmgr_id,
          "logfile": utils.make_shell_logfiles_url(host, shell_port, cwd, instance.instance_id),
      }
      physical_plan["stmgrs"][stmgr_id]["instance_ids"].append(instance_id)
      if name in physical_plan["spouts"]:
        physical_plan["spouts"][name].append(instance_id)
      else:
        physical_plan["bolts"][name].append(instance_id)

    physical_plan["instance_groups"] = instance_groups

    return physical_plan

  # pylint: disable=too-many-locals
  def extract_packing_plan(self, topology):
    """
    Returns the representation of packing plan that will be returned from Tracker.

    """
    packing_plan = {
        "id": "",
        "container_plans": []
    }

    if not topology.packing_plan:
      return packing_plan

    packing_plan["id"] = topology.packing_plan.id
    packing_plan["container_plans"] = [
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

    return packing_plan

  def pb2_to_api(self, topology: topology_pb2.Topology) -> Dict[str, Any]:
    """
    Returns the JSON marshalled form of a topology.

    This uses a cache that is managed by the tracker so that the transform
    only happens once for those Topology instances. Bear this in mind if
    you want to modify the results - it is probably better to make those
    changes in the Tracker._pb2_to_api and its dependencies.

    """
    topology_info = self._pb2_to_api_cache.get(topology)
    if topology_info is None:
      # this happens if the Toplogy instance was not managed by the tracker
      topology_info = self._pb2_to_api(topology)
      self._pb2_to_api_cache[topology] = topology_info
    return topology_info
