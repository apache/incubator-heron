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

'''pplan_helper.py'''
import socket

from heron.proto import topology_pb2
from heron.common.src.python.utils.log import Log
import heron.common.src.python.pex_loader as pex_loader
from heron.instance.src.python.utils.topology import TopologyContextImpl

from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.api.serializer import default_serializer

from .custom_grouping_helper import CustomGroupingHelper

# pylint: disable=too-many-instance-attributes
class PhysicalPlanHelper(object):
  """Helper class for accessing Physical Plan

  :ivar pplan: Physical Plan protobuf message
  :ivar topology_pex_abs_path: Topology pex file's absolute path
  :ivar my_instance_id: instance id for this instance
  :ivar my_instance: Instance protobuf message for this instance
  :ivar my_component_name: component name for this instance
  :ivar my_task_id: global task id for this instance
  :ivar is_spout: ``True`` if it's spout, ``False`` if it's bolt
  :ivar hostname: hostname of this instance
  :ivar my_component: Component protobuf message for this instance
  :ivar context: Topology context if set, otherwise ``None``
  """
  def __init__(self, pplan, instance_id, topology_pex_abs_path):
    self.pplan = pplan
    self.my_instance_id = instance_id
    self.my_instance = None
    self.topology_pex_abs_path = topology_pex_abs_path

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

    # setup output schema
    for out_stream in outputs:
      self._output_schema[out_stream.stream.id] = len(out_stream.schema.keys)

    self.hostname = socket.gethostname()
    self.my_component = self._my_spbl.comp

    self.context = None

    # setup for custom grouping
    self.custom_grouper = CustomGroupingHelper()
    self._setup_custom_grouping(pplan.topology)

  def _get_my_spout_or_bolt(self, topology):
    my_spbl = None
    for spbl in list(topology.spouts) + list(topology.bolts):
      if spbl.comp.name == self.my_component_name:
        if my_spbl is not None:
          raise RuntimeError("Duplicate my component found")
        my_spbl = spbl

    Log.info(my_spbl.__class__.__name__)

    if isinstance(my_spbl, topology_pb2.Spout):
      is_spout = True
    elif isinstance(my_spbl, topology_pb2.Bolt):
      is_spout = False
    elif my_spbl.__class__.__name__ == "Spout":
      Log.info("Mismatch between cpp and python protobuf")
      is_spout = True
    elif my_spbl.__class__.__name__ == "Bolt":
      Log.info("Mismatch between cpp and python protobuf")
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

  def is_topology_paused(self):
    """Checks whether topology is currently paused"""
    return self.pplan.topology.state == topology_pb2.TopologyState.Value("PAUSED")

  def is_topology_killed(self):
    """Checks whether topology is already killed"""
    return self.pplan.topology.state == topology_pb2.TopologyState.Value("KILLED")

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
    self.context = TopologyContextImpl(cluster_config, self.pplan.topology, task_to_component_map,
                                       self.my_task_id, metrics_collector,
                                       self.topology_pex_abs_path)

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
    config = {}
    for kv in topology_config.kvs:
      if kv.HasField("value"):
        assert kv.type == topology_pb2.ConfigValueType.Value("STRING_VALUE")
        # value is string
        if PhysicalPlanHelper._is_number(kv.value):
          config[kv.key] = PhysicalPlanHelper._get_number(kv.value)
        elif kv.value.lower() in ("true", "false"):
          config[kv.key] = True if kv.value.lower() == "true" else False
        else:
          config[kv.key] = kv.value
      elif kv.HasField("serialized_value") and \
        kv.type == topology_pb2.ConfigValueType.Value("PYTHON_SERIALIZED_VALUE"):
        # deserialize that
        config[kv.key] = default_serializer.deserialize(kv.serialized_value)
      else:
        assert kv.HasField("type")
        Log.error("Unsupported config <key:value> found: %s, with type: %s"
                  % (str(kv), str(kv.type)))
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

  ##### custom grouping related #####

  def _setup_custom_grouping(self, topology):
    """Checks whether there are any bolts that consume any of my streams using custom grouping"""
    for i in range(len(topology.bolts)):
      for in_stream in topology.bolts[i].inputs:
        if in_stream.stream.component_name == self.my_component_name and \
          in_stream.gtype == topology_pb2.Grouping.Value("CUSTOM"):
          # this bolt takes my output in custom grouping manner
          if in_stream.type == topology_pb2.CustomGroupingObjectType.Value("PYTHON_OBJECT"):
            custom_grouping_obj = default_serializer.deserialize(in_stream.custom_grouping_object)
            if isinstance(custom_grouping_obj, str):
              pex_loader.load_pex(self.topology_pex_abs_path)
              grouping_cls = \
                pex_loader.import_and_get_class(self.topology_pex_abs_path, custom_grouping_obj)
              custom_grouping_obj = grouping_cls()
            assert isinstance(custom_grouping_obj, ICustomGrouping)
            self.custom_grouper.add(in_stream.stream.id,
                                    self._get_taskids_for_component(topology.bolts[i].comp.name),
                                    custom_grouping_obj,
                                    self.my_component_name)

          elif in_stream.type == topology_pb2.CustomGroupingObjectType.Value("JAVA_OBJECT"):
            raise NotImplementedError("Java-serialized custom grouping is not yet supported "
                                      "for python topology")
          else:
            raise ValueError("Unrecognized custom grouping type found: %s" % str(in_stream.type))

  def _get_taskids_for_component(self, component_name):
    return [instance.info.task_id for instance in self.pplan.instances
            if instance.info.component_name == component_name]

  def prepare_custom_grouping(self, context):
    """Prepares for custom grouping for this component

    :param context: Topology context
    """
    self.custom_grouper.prepare(context)

  def choose_tasks_for_custom_grouping(self, stream_id, values):
    """Choose target task ids for custom grouping

    :return: task ids
    """
    return self.custom_grouper.choose_tasks(stream_id, values)
