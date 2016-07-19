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
import socket

from heron.proto import topology_pb2
from heron.common.src.python.log import Log
from heron.common.src.python.utils.topology_context import TopologyContext

class PhysicalPlanHelper(object):
  """Helper class for accessing Physical Plan

  :ivar pplan: Physical Plan object
  :ivar my_instance_id: instance id for this instance
  :ivar my_instance: instance object for this instance
  :ivar my_component_name: component name for this instance
  :ivar my_task_id: global task id for this instance
  :ivar is_spout: ``True`` if it's spout, ``False`` if it's bolt
  """
  def __init__(self, pplan, instance_id):
    self.pplan = pplan
    self.my_instance_id = instance_id
    self.my_instance = None

    # get my instance
    for instance in pplan.instances:
      if instance.instance_id == self.my_instance_id:
        self.my_instance = instance
        break

    if self.my_instance is None:
      raise RuntimeError("There was no instance that matched my id " + self.my_instance_id)

    self.my_component_name = self.my_instance.info.component_name
    self.my_task_id = self.my_instance.info.task_id

    # get spout or bolt
    self.my_spbl, self.is_spout = self._get_my_spout_or_bolt(pplan.topology)

    # output schema
    self.output_schema = dict()
    outputs = self.my_spbl.outputs

    for out_stream in outputs:
      self.output_schema[out_stream.stream.id] = len(out_stream.schema.keys)

    self.hostname = socket.gethostname()
    self.my_component = self.my_spbl.comp

    self.context = None

    # TODO: topology.proto -- Component needs to change: Add python_class_name
    # TODO: implement CustomGrouping stuff

  def _get_my_spout_or_bolt(self, topology):
    my_spbl = None
    for spbl in (list(topology.spouts) + list(topology.bolts)):
      if spbl.comp.name == self.my_component_name:
        if my_spbl is not None:
          raise RuntimeError("Duplicate my component found")
        my_spbl = spbl

    if isinstance(my_spbl, topology_pb2.Spout):
      is_spout = True
    elif isinstance(my_spbl, topology_pb2.Bolt):
      is_spout = False
    else:
      raise RuntimeError("My component neither spout nor bolt")
    return my_spbl, is_spout

  def get_my_spout(self):
    if self.is_spout:
      return self.my_spbl
    else:
      return None

  def get_my_bolt(self):
    if self.is_spout:
      return None
    else:
      return self.my_spbl

  def get_topology_state(self):
    return self.pplan.topology.state

  def is_topology_running(self):
    return self.pplan.topology.state == topology_pb2.TopologyState.Value("RUNNING")

  def get_topology_config(self):
    if self.pplan.topology.HasField("topology_config"):
      return self._get_dict_from_config(self.pplan.topology.topology_config)
    else:
      return {}

  def set_topology_context(self, metrics_collector):
    Log.debug("Setting topology context")
    cluster_config = self.get_topology_config()
    cluster_config.update(self._get_dict_from_config(self.my_component.config))
    task_to_component_map = self._get_task_to_comp_map()
    self.context = TopologyContext(cluster_config, self.pplan.topology, task_to_component_map,
                                   self.my_task_id, metrics_collector)

  @staticmethod
  def _get_dict_from_config(topology_config):
    config = {}
    for kv in topology_config.kvs:
      if kv.HasField("value"):
        config[kv.key] = kv.value
      else:
        Log.error("Unsupported config key:value found: " + str(kv))
        continue

    return config

  def _get_task_to_comp_map(self):
    ret = {}
    for instance in self.pplan.instances:
      ret[instance.info.task_id] = instance.info.component_name
    return ret

