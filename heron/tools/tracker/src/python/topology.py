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
import dataclasses
import json
import string
import threading

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from copy import deepcopy
import networkx

from pydantic import BaseModel, Field

from heron.proto import topology_pb2
from heron.proto.execution_state_pb2 import ExecutionState as ExecutionState_pb
from heron.proto.packing_plan_pb2 import PackingPlan as PackingPlan_pb
from heron.proto.physical_plan_pb2 import PhysicalPlan as PhysicalPlan_pb
from heron.proto.scheduler_pb2 import SchedulerLocation as SchedulerLocation_pb
from heron.proto.tmanager_pb2 import TManagerLocation as TManagerLocation_pb
from heron.tools.tracker.src.python.config import (
    Config,
    EXTRA_LINK_FORMATTER_KEY,
    EXTRA_LINK_URL_KEY,
)
from heron.tools.tracker.src.python import utils

class TopologyInfoMetadata(BaseModel):
  cluster: str
  environ: str
  role: str
  jobname: str
  submission_time: int
  submission_user: str
  release_username: str
  release_tag: str
  release_version: str
  instances: int = 0
  extra_links: List[Dict[str, str]]

class TopologyInfoExecutionState(TopologyInfoMetadata):
  """
  This model is a superset of the "metadata".

  Note: this may be a symptom of a bad pattern, the presence of these
      things could be determined by making their respective objects
      optional rather than empty
  """
  has_physical_plan: bool
  has_packing_plan: bool
  has_tmanager_location: bool
  has_scheduler_location: bool
  status: str

class RuntimeStateStatemanager(BaseModel):
  is_registered: bool

class TopologyInfoRuntimeState(BaseModel):
  has_physical_plan: bool
  has_packing_plan: bool
  has_tmanager_location: bool
  has_scheduler_location: bool
  stmgrs: Dict[str, RuntimeStateStatemanager] = Field(
      ...,
      deprecated=True,
      description="this is only populated by the /topologies/runtimestate endpoint",
  )


class TopologyInfoSchedulerLocation(BaseModel):
  name: Optional[str]
  http_endpoint: Optional[str]
  job_page_link: Optional[str] = Field(None, description="may be empty")

class TopologyInfoTmanagerLocation(BaseModel):
  name: Optional[str]
  id: Optional[str]
  host: Optional[str]
  controller_port: Optional[int]
  server_port: Optional[int]
  stats_port: Optional[int]

class PackingPlanRequired(BaseModel):
  cpu: float
  ram: int
  disk: int

class PackingPlanScheduled(BaseModel):
  cpu: Optional[float]
  ram: Optional[int]
  disk: Optional[int]

class PackingPlanInstance(BaseModel):
  component_name: str
  task_id: int
  component_index: int
  instance_resources: PackingPlanRequired

class PackingPlanContainer(BaseModel):
  id: int
  instances: List[PackingPlanInstance]
  required_resources: PackingPlanRequired
  scheduled_resources: PackingPlanScheduled

class TopologyInfoPackingPlan(BaseModel):
  id: str
  container_plans: List[PackingPlanContainer]

class PhysicalPlanStmgr(BaseModel):
  id: str
  host: str
  port: int
  shell_port: int
  cwd: str
  pid: int
  joburl: str
  logfiles: str = Field(..., description="URL to retrieve logs")
  instance_ids: List[str]

class PhysicalPlanInstance(BaseModel):
  id: str
  name: str
  stmgr_id: str
  logfile: str = Field(..., description="URL to retrieve log")

class PhysicalPlanComponent(BaseModel):
  config: Dict[str, Any]

class TopologyInfoPhysicalPlan(BaseModel):
  instances: Dict[str, PhysicalPlanInstance]
  # instance_id is in the form <container>_<container index>_<component>_<component index>
  # the container is the "group"
  instance_groups: Dict[str, List[str]] = Field(
      ...,
      description="map of instance group name to instance ids",
  )
  stmgrs: Dict[str, PhysicalPlanStmgr] = Field(..., description="map of stmgr id to stmgr info")
  spouts: Dict[str, List[str]] = Field(..., description="map of name to instance ids")
  bolts: Dict[str, List[str]] = Field(..., description="map of name to instance ids")
  config: Dict[str, Any]
  components: Dict[str, PhysicalPlanComponent] = Field(
      ...,
      description="map of bolt/spout name to info",
  )

class LogicalPlanStream(BaseModel):
  name: str = Field(..., alias="stream_name")

class LogicalPlanBoltInput(BaseModel):
  stream_name: str
  component_name: str
  grouping: str

class LogicalPlanBolt(BaseModel):
  config: Dict[str, Any]
  outputs: List[LogicalPlanStream]
  inputs: List[LogicalPlanBoltInput]
  input_components: List[str] = Field(..., alias="inputComponents", deprecated=True)

class LogicalPlanSpout(BaseModel):
  config: Dict[str, Any]
  type: str = Field(..., alias="spout_type")
  source: str = Field(..., alias="spout_source")
  version: str
  outputs: List[LogicalPlanStream]
  extra_links: List[Dict[str, Any]]

class TopologyInfoLogicalPlan(BaseModel):
  bolts: Dict[str, LogicalPlanBolt]
  spouts: Dict[str, LogicalPlanSpout]
  stages: int = Field(..., description="number of components in longest path")

class TopologyInfo(BaseModel):
  execution_state: TopologyInfoExecutionState
  id: Optional[str]
  logical_plan: TopologyInfoLogicalPlan
  metadata: TopologyInfoMetadata
  name: str
  packing_plan: TopologyInfoPackingPlan
  physical_plan: TopologyInfoPhysicalPlan
  runtime_state: TopologyInfoRuntimeState
  scheduler_location: TopologyInfoSchedulerLocation
  tmanager_location: TopologyInfoTmanagerLocation


def topology_stages(logical_plan: TopologyInfoLogicalPlan) -> int:
  """Return the number of stages in a logical plan."""
  graph = networkx.DiGraph(
      (input_info.component_name, bolt_name)
      for bolt_name, bolt_info in logical_plan.bolts.items()
      for input_info in bolt_info.inputs
  )
  # this is is the same as "diameter" if treating the topology as an undirected graph
  return networkx.dag_longest_path_length(graph)

@dataclasses.dataclass(frozen=True)
class TopologyState:
  """Collection of state accumulated for tracker from state manager."""
  tmanager: Optional[TManagerLocation_pb]
  scheduler_location: Optional[SchedulerLocation_pb]
  physical_plan: Optional[PhysicalPlan_pb]
  packing_plan: Optional[PackingPlan_pb]
  execution_state: Optional[ExecutionState_pb]

# pylint: disable=too-many-instance-attributes
@dataclass(init=False)
class Topology:
  """
  Class Topology
    Contains all the relevant information about
    a topology that its state manager has.

    All this info is fetched from state manager in one go.

  """

  def __init__(self, name: str, state_manager_name: str, tracker_config: Config) -> None:
    self.name = name
    self.state_manager_name = state_manager_name
    self.physical_plan: Optional[PhysicalPlan_pb] = None
    self.packing_plan: Optional[PackingPlan_pb] = None
    self.tmanager: Optional[TManagerLocation_pb] = None
    self.scheduler_location: Optional[SchedulerLocation_pb] = None
    self.execution_state: Optional[ExecutionState_pb] = None
    self.id: Optional[int] = None
    self.tracker_config: Config = tracker_config
    # this maps pb2 structs to structures returned via API endpoints
    # it is repopulated every time one of the pb2 properties is updated
    self.info: Optional[TopologyInfo] = None
    self.lock = threading.RLock()

  def __eq__(self, o):
    return isinstance(o, Topology) \
           and o.name == self.name \
           and o.state_manager_name == self.state_manager_name \
           and o.cluster == self.cluster \
           and o.environ == self.environ

  @staticmethod
  def _render_extra_links(extra_links, topology, execution_state: ExecutionState_pb) -> None:
    """Render links in place."""
    subs = {
        "CLUSTER": execution_state.cluster,
        "ENVIRON": execution_state.environ,
        "ROLE": execution_state.role,
        "TOPOLOGY": topology.name,
        "USER": execution_state.submission_user,
    }
    for link in extra_links:
      link[EXTRA_LINK_URL_KEY] = string.Template(link[EXTRA_LINK_FORMATTER_KEY]).substitute(subs)

  def _rebuild_info(self, t_state: TopologyState) -> Optional[TopologyInfo]:
    # Execution state is the most basic info. If return execution state, just return
    # as the rest of the things don't matter.
    execution_state = t_state.execution_state
    if not execution_state:
      return None
    # take references to instances to reduce inconsistency risk, which would
    # be a problem if the topology is updated in the middle of a call to this

    topology = self
    packing_plan = t_state.packing_plan
    physical_plan = t_state.physical_plan
    tmanager = t_state.tmanager
    scheduler_location = t_state.scheduler_location
    tracker_config = self.tracker_config # assuming this is never updated
    return TopologyInfo(
        id=topology.id,
        logical_plan=self._build_logical_plan(topology, execution_state, physical_plan),
        metadata=self._build_metadata(topology, physical_plan, execution_state, tracker_config),
        name=topology.name, # was self.name
        packing_plan=self._build_packing_plan(packing_plan),
        physical_plan=self._build_physical_plan(physical_plan),
        runtime_state=self._build_runtime_state(
            physical_plan=physical_plan,
            packing_plan=packing_plan,
            tmanager=tmanager,
            scheduler_location=scheduler_location,
            execution_state=execution_state,
        ),
        execution_state=self._build_execution_state(
            topology=topology,
            execution_state=execution_state,
            physical_plan=physical_plan,
            packing_plan=packing_plan,
            tmanager=tmanager,
            scheduler_location=scheduler_location,
            tracker_config=tracker_config,
        ),
        scheduler_location=self._build_scheduler_location(scheduler_location),
        tmanager_location=self._build_tmanager_location(tmanager),
    )

  @staticmethod
  def _build_execution_state(
      topology,
      execution_state,
      physical_plan,
      packing_plan,
      tmanager,
      scheduler_location,
      tracker_config,
    ) -> TopologyInfoExecutionState:
    status = {
        topology_pb2.RUNNING: "Running",
        topology_pb2.PAUSED: "Paused",
        topology_pb2.KILLED: "Killed",
    }.get(physical_plan.topology.state if physical_plan else None, "Unknown")
    metadata = Topology._build_metadata(topology, physical_plan, execution_state, tracker_config)
    return TopologyInfoExecutionState(
        has_physical_plan=bool(physical_plan),
        has_packing_plan=bool(packing_plan),
        has_tmanager_location=bool(tmanager),
        has_scheduler_location=bool(scheduler_location),
        status=status,
        **metadata.dict(),
    )

  @staticmethod
  def _build_logical_plan(
      topology: "Topology",
      execution_state: ExecutionState_pb,
      physical_plan: Optional[PhysicalPlan_pb],
    ) -> TopologyInfoLogicalPlan:
    if not physical_plan:
      return TopologyInfoLogicalPlan(spouts={}, bolts={}, stages=0)
    spouts = {}
    for spout in physical_plan.topology.spouts:
      config = utils.convert_pb_kvs(spout.comp.config.kvs, include_non_primitives=False)
      extra_links = json.loads(config.get("extra.links", "[]"))
      Topology._render_extra_links(extra_links, topology, execution_state)
      spouts[spout.comp.name] = LogicalPlanSpout(
          config=config,
          spout_type=config.get("spout.type", "default"),
          spout_source=config.get("spout.source", "NA"),
          version=config.get("spout.version", "NA"),
          extra_links=extra_links,
          outputs=[
              LogicalPlanStream(stream_name=output.stream.id)
              for output in spout.outputs
          ],
      )

    info = TopologyInfoLogicalPlan(
        stages=0,
        spouts=spouts,
        bolts={
            bolt.comp.name: LogicalPlanBolt(
                config=utils.convert_pb_kvs(bolt.comp.config.kvs, include_non_primitives=False),
                outputs=[
                    LogicalPlanStream(stream_name=output.stream.id)
                    for output in bolt.outputs
                ],
                inputs=[
                    LogicalPlanBoltInput(
                        stream_name=input_.stream.id,
                        component_name=input_.stream.component_name,
                        grouping=topology_pb2.Grouping.Name(input_.gtype),
                    )
                    for input_ in bolt.inputs
                ],
                inputComponents=[
                    input_.stream.component_name
                    for input_ in bolt.inputs
                ],
            )
            for bolt in physical_plan.topology.bolts
        },
    )
    info.stages = topology_stages(info)
    return info

  @staticmethod
  def _build_metadata(topology, physical_plan, execution_state, tracker_config) \
          -> TopologyInfoMetadata:
    if not execution_state:
      return  TopologyInfoMetadata()
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
    }
    if physical_plan is not None and hasattr(physical_plan, "instances"):
      metadata["instances"] = len(physical_plan.instances)
    extra_links = deepcopy(tracker_config.extra_links)
    Topology._render_extra_links(extra_links, topology, execution_state)
    return TopologyInfoMetadata(
        extra_links=extra_links,
        **metadata,
    )

  @staticmethod
  def _build_packing_plan(packing_plan) -> TopologyInfoPackingPlan:
    if not packing_plan:
      return TopologyInfoPackingPlan(id="", container_plans=[])
    return TopologyInfoPackingPlan(
        id=packing_plan.id,
        container_plans=[
            PackingPlanContainer(
                id=container.id,
                instances=[
                    PackingPlanInstance(
                        component_name=instance.component_name,
                        task_id=instance.task_id,
                        component_index=instance.component_index,
                        instance_resources=PackingPlanRequired(
                            cpu=instance.resource.cpu,
                            ram=instance.resource.ram,
                            disk=instance.resource.disk,
                        ),
                    )
                    for instance in container.instance_plans
                ],
                required_resources=PackingPlanRequired(
                    cpu=container.requiredResource.cpu,
                    ram=container.requiredResource.ram,
                    disk=container.requiredResource.disk,
                ),
                scheduled_resources=(
                    PackingPlanScheduled(
                        cpu=container.scheduledResource.cpu,
                        ram=container.scheduledResource.ram,
                        disk=container.scheduledResource.ram,
                    )
                    if container.scheduledResource else
                    PackingPlanScheduled()
                ),
            )
            for container in packing_plan.container_plans
        ],
    )

  @staticmethod
  def _build_physical_plan(physical_plan) -> TopologyInfoPhysicalPlan:
    if not physical_plan:
      return TopologyInfoPhysicalPlan(
          instances={},
          instance_groups={},
          stmgrs={},
          spouts={},
          bolts={},
          config={},
          components={},
      )
    config = {}
    if physical_plan.topology.topology_config:
      config = utils.convert_pb_kvs(physical_plan.topology.topology_config.kvs)

    components = {}
    spouts = {}
    bolts = {}
    for spout in physical_plan.topology.spouts:
      name = spout.comp.name
      spouts[name] = []
      if name not in components:
        components[name] = PhysicalPlanComponent(
            config=utils.convert_pb_kvs(spout.comp.config.kvs),
        )
    for bolt in physical_plan.topology.bolts:
      name = bolt.comp.name
      bolts[name] = []
      if name not in components:
        components[name] = PhysicalPlanComponent(
            config=utils.convert_pb_kvs(bolt.comp.config.kvs),
        )

    stmgrs = {}
    for stmgr in physical_plan.stmgrs:
      shell_port = stmgr.shell_port if stmgr.HasField("shell_port") else None
      stmgrs[stmgr.id] = PhysicalPlanStmgr(
          id=stmgr.id,
          host=stmgr.host_name,
          port=stmgr.data_port,
          shell_port=shell_port,
          cwd=stmgr.cwd,
          pid=stmgr.pid,
          joburl=utils.make_shell_job_url(stmgr.host_name, shell_port, stmgr.cwd),
          logfiles=utils.make_shell_logfiles_url(stmgr.host_name, stmgr.shell_port, stmgr.cwd),
          instance_ids=[],
      )

    instances = {}
    instance_groups = {}
    for instance in physical_plan.instances:
      component_name = instance.info.component_name
      instance_id = instance.instance_id
      if component_name in spouts:
        spouts[component_name].append(instance_id)
      else:
        bolts[component_name].append(instance_id)

      stmgr = stmgrs[instance.stmgr_id]
      stmgr.instance_ids.append(instance_id)
      instances[instance_id] = PhysicalPlanInstance(
          id=instance_id,
          name=component_name,
          stmgr_id=instance.stmgr_id,
          logfile=utils.make_shell_logfiles_url(
              stmgr.host,
              stmgr.shell_port,
              stmgr.cwd,
              instance_id,
          ),
      )

      # instance_id example: container_1_component_1
      # group name would be: container_1
      group_name = instance_id.rsplit("_", 2)[0]
      instance_groups.setdefault(group_name, []).append(instance_id)

    return TopologyInfoPhysicalPlan(
        instances=instances,
        instance_groups=instance_groups,
        stmgrs=stmgrs,
        spouts=spouts,
        bolts=bolts,
        components=components,
        config=config,
    )

  @staticmethod
  def _build_runtime_state(
      physical_plan,
      packing_plan,
      tmanager,
      scheduler_location,
      execution_state,
    ) -> TopologyInfoRuntimeState:
    return TopologyInfoRuntimeState(
        has_physical_plan=bool(physical_plan),
        has_packing_plan=bool(packing_plan),
        has_tmanager_location=bool(tmanager),
        has_scheduler_location=bool(scheduler_location),
        release_version=execution_state.release_state.release_version,
        stmgrs={},
    )

  @staticmethod
  def _build_scheduler_location(scheduler_location) -> TopologyInfoSchedulerLocation:
    if not scheduler_location:
      return TopologyInfoSchedulerLocation(name=None, http_endpoint=None, job_page_link=None)
    return TopologyInfoSchedulerLocation(
        name=scheduler_location.topology_name,
        http_endpoint=scheduler_location.http_endpoint,
        job_page_link=scheduler_location.job_page_link[0] \
            if scheduler_location.job_page_link else "",
    )

  @staticmethod
  def _build_tmanager_location(tmanager) -> TopologyInfoTmanagerLocation:
    if not tmanager:
      return TopologyInfoTmanagerLocation(
          name=None,
          id=None,
          host=None,
          controller_port=None,
          server_port=None,
          stats_port=None,
      )
    return TopologyInfoTmanagerLocation(
        name=tmanager.topology_name,
        id=tmanager.topology_id,
        host=tmanager.host,
        controller_port=tmanager.controller_port,
        server_port=tmanager.server_port,
        status_port=tmanager.stats_port,
    )

  def _update(
      self,
      physical_plan=...,
      packing_plan=...,
      execution_state=...,
      tmanager=...,
      scheduler_location=...,
    ) -> None:
    """Atomically update this instance to avoid inconsistent reads/writes from other threads."""
    with self.lock:
      t_state = TopologyState(
          physical_plan=self.physical_plan if physical_plan is ... else physical_plan,
          packing_plan=self.packing_plan if packing_plan is ... else packing_plan,
          execution_state=self.execution_state if execution_state is ... else execution_state,
          tmanager=self.tmanager if tmanager is ... else tmanager,
          scheduler_location=self.scheduler_location \
              if scheduler_location is ... else scheduler_location,
      )
      if t_state.physical_plan:
        id_ = t_state.physical_plan.topology.id
      elif t_state.packing_plan:
        id_ = t_state.packing_plan.id
      else:
        id_ = None

      info = self._rebuild_info(t_state)
      update = dataclasses.asdict(t_state)
      update["info"] = info
      update["id"] = id_
      if t_state.execution_state:
        update["cluster"] = t_state.execution_state.cluster
        update["environ"] = t_state.execution_state.environ
      # atomic update using python GIL
      self.__dict__.update(update)

  def set_physical_plan(self, physical_plan: PhysicalPlan_pb) -> None:
    """ set physical plan """
    self._update(physical_plan=physical_plan)

  def set_packing_plan(self, packing_plan: PackingPlan_pb) -> None:
    """ set packing plan """
    self._update(packing_plan=packing_plan)

  def set_execution_state(self, execution_state: ExecutionState_pb) -> None:
    """ set exectuion state """
    self._update(execution_state=execution_state)

  def set_tmanager(self, tmanager: TManagerLocation_pb) -> None:
    """ set exectuion state """
    self._update(tmanager=tmanager)

  def set_scheduler_location(self, scheduler_location: SchedulerLocation_pb) -> None:
    """ set exectuion state """
    self._update(scheduler_location=scheduler_location)

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

  def spouts(self):
    """
    Returns a list of Spout (proto) messages
    """
    physical_plan = self.physical_plan
    if physical_plan:
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
    physical_plan = self.physical_plan
    if physical_plan:
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
    physical_plan = self.physical_plan
    if physical_plan:
      return [s.host_name for s in physical_plan.stmgrs]
    return []
