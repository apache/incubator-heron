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
'''pplan_helper.py'''
import socket

from heron.proto import topology_pb2
from heron.common.src.python.log import Log
from heron.common.src.python.utils.topology import TopologyContext

from .serializer import PythonSerializer

# pylint: disable=too-many-instance-attributes
class PhysicalPlanHelper(object):
  """Helper class for accessing Physical Plan

  :ivar pplan: Physical Plan protobuf message
  :ivar my_instance_id: instance id for this instance
  :ivar my_instance: Instance protobuf message for this instance
  :ivar my_component_name: component name for this instance
  :ivar my_task_id: global task id for this instance
  :ivar is_spout: ``True`` if it's spout, ``False`` if it's bolt
  :ivar hostname: hostname of this instance
  :ivar my_component: Component protobuf message for this instance
  :ivar context: Topology context if set, otherwise ``None``
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
      raise RuntimeError("There was no instance that matched my id: %s" % self.my_instance_id)

    self.my_component_name = self.my_instance.info.component_name
    self.my_task_id = self.my_instance.info.task_id

    # get spout or bolt
    self._my_spbl, self.is_spout = self._get_my_spout_or_bolt(pplan.topology)

    # Map <stream id -> number of fields in that stream's schema>
    self._output_schema = dict()
    outputs = self._my_spbl.outputs

    for out_stream in outputs:
      self._output_schema[out_stream.stream.id] = len(out_stream.schema.keys)

    self.hostname = socket.gethostname()
    self.my_component = self._my_spbl.comp

    self.context = None

    # TODO: implement CustomGrouping stuff

  def _get_my_spout_or_bolt(self, topology):
    my_spbl = None
    for spbl in list(topology.spouts) + list(topology.bolts):
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

  def check_output_schema(self, stream_id, tup):
    """Checks if a given stream_id and tuple matches with the output schema

    :type stream_id: str
    :param stream_id: stream id into which tuple is sent
    :type tup: list
    :param tup: tuple that is going to be sent
    """
    # do some checking to make sure that the number of fields match what's expected
    size = self._output_schema.get(stream_id, None)
    if size is None:
      raise RuntimeError("%s emitting to stream %s but was not declared in output fields"
                         % (self.my_component_name, stream_id))
    elif size != len(tup):
      raise RuntimeError("Number of fields emitted in stream %s does not match what's expected. "
                         "Expected: %s, Observed: %s" % (stream_id, size, len(tup)))

  def get_my_spout(self):
    """Returns spout instance, or ``None`` if bolt is assigned"""
    if self.is_spout:
      return self._my_spbl
    else:
      return None

  def get_my_bolt(self):
    """Returns bolt instance, or ``None`` if spout is assigned"""
    if self.is_spout:
      return None
    else:
      return self._my_spbl

  def get_topology_state(self):
    """Returns the current topology state"""
    return self.pplan.topology.state

  def is_topology_running(self):
    """Checks whether topology is currently running"""
    return self.pplan.topology.state == topology_pb2.TopologyState.Value("RUNNING")

  def get_topology_config(self):
    """Returns the topology config"""
    if self.pplan.topology.HasField("topology_config"):
      return self._get_dict_from_config(self.pplan.topology.topology_config)
    else:
      return {}

  def set_topology_context(self, metrics_collector):
    """Sets a new topology context"""
    Log.debug("Setting topology context")
    cluster_config = self.get_topology_config()
    cluster_config.update(self._get_dict_from_config(self.my_component.config))
    task_to_component_map = self._get_task_to_comp_map()
    self.context = TopologyContext(cluster_config, self.pplan.topology, task_to_component_map,
                                   self.my_task_id, metrics_collector)

  @staticmethod
  def _get_dict_from_config(topology_config):
    """Converts Config protobuf message to python dictionary

    Values are converted according to the rules below:

    - Number string (e.g. "12" or "1.2") is appropriately converted to ``int`` or ``float``
    - Boolean string ("true", "True", "false" or "False") is converted to built-in boolean type
      (i.e. ``True`` or ``False``)
    - Normal string is inserted to dict as is
    - Serialized value is deserialized and inserted as a corresponding Python object
    """
    # TODO: write unittest
    config = {}
    config_serializer = PythonSerializer()
    for kv in topology_config.kvs:
      if kv.HasField("value"):
        # value is string
        if PhysicalPlanHelper._is_number(kv.value):
          config[kv.key] = PhysicalPlanHelper._get_number(kv.value)
        elif kv.value in ("true", "True", "false", "False"):
          config[kv.key] = True if kv.value in ("true", "True") else False
        else:
          config[kv.key] = kv.value
      elif kv.HasField("serialized_value") and \
        kv.type == topology_pb2.ConfigValueType.Value("PYTHON_SERIALIZED_VALUE"):
        # deserialize that
        config[kv.key] = config_serializer.deserialize(kv.serialized_value)
      else:
        Log.error("Unsupported config <key:value> found: %s" % str(kv))
        continue

    return config

  @staticmethod
  def _is_number(string):
    try:
      float(string)
      return True
    except ValueError:
      return False

  @staticmethod
  def _get_number(string):
    try:
      return int(string)
    except ValueError:
      return float(string)

  def _get_task_to_comp_map(self):
    ret = {}
    for instance in self.pplan.instances:
      ret[instance.info.task_id] = instance.info.component_name
    return ret
