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

'''topology_context.py'''
import os
from collections import namedtuple

from heronpy.api.task_hook import (ITaskHook, EmitInfo, SpoutAckInfo,
                                   SpoutFailInfo, BoltExecuteInfo,
                                   BoltAckInfo, BoltFailInfo)
from heronpy.api.topology_context import TopologyContext

import heronpy.api.api_constants as api_constants
from heron.instance.src.python.utils.metrics import MetricsCollector

import heron.instance.src.python.utils.system_constants as system_constants
import heron.common.src.python.pex_loader as pex_loader

class TopologyContextImpl(TopologyContext):
  """Implemention of TopologyContext

  This is created by Heron Instance and passed on to the topology spouts/bolts
  as the topology context

  """
  # pylint: disable=too-many-instance-attributes

  def __init__(self, config, topology, task_to_component, my_task_id, metrics_collector,
               topo_pex_path):
    self.config = config
    self.topology = topology
    self.task_to_component_map = task_to_component
    self.task_id = my_task_id
    self.metrics_collector = metrics_collector
    self.topology_pex_path = os.path.abspath(topo_pex_path)

    inputs, outputs, out_fields = self._get_inputs_and_outputs_and_outfields(topology)
    self.inputs = inputs
    self.outputs = outputs
    self.component_to_out_fields = out_fields

    # init task hooks
    self.task_hooks = []
    self._init_task_hooks()

  ##### Implementation of interface methods #####

  def get_task_id(self):
    """Property to get the task id of this component"""
    return self.task_id

  def get_component_id(self):
    """Property to get the component id of this component"""
    return self.task_to_component_map.get(self.get_task_id())

  def get_cluster_config(self):
    """Returns the cluster config for this component

    Note that the returned config is auto-typed map: <str -> any Python object>.
    """
    return self.config

  def get_topology_name(self):
    """Returns the name of the topology
    """
    return str(self.topology.name)

  def register_metric(self, name, metric, time_bucket_in_sec):
    """Registers a new metric to this context"""
    collector = self.get_metrics_collector()
    collector.register_metric(name, metric, time_bucket_in_sec)

  def get_sources(self, component_id):
    """Returns the declared inputs to specified component

    :return: map <streamId namedtuple (same structure as protobuf msg) -> gtype>, or
             None if not found
    """
    # this is necessary because protobuf message is not hashable
    StreamId = namedtuple('StreamId', 'id, component_name')
    if component_id in self.inputs:
      ret = {}
      for istream in self.inputs.get(component_id):
        key = StreamId(id=istream.stream.id, component_name=istream.stream.component_name)
        ret[key] = istream.gtype
      return ret
    else:
      return None

  def get_this_sources(self):
    return self.get_sources(self.get_component_id())

  def get_component_tasks(self, component_id):
    """Returns the task ids allocated for the given component id"""
    ret = []
    for task_id, comp_id in list(self.task_to_component_map.items()):
      if comp_id == component_id:
        ret.append(task_id)
    return ret

  def add_task_hook(self, task_hook):
    """Registers a specified task hook to this context

    :type task_hook: heron.instance.src.python.utils.topology.ITaskHook
    :param task_hook: Implementation of ITaskHook
    """
    if not isinstance(task_hook, ITaskHook):
      raise TypeError("In add_task_hook(): attempt to add non ITaskHook instance, given: %s"
                      % str(type(task_hook)))
    self.task_hooks.append(task_hook)

  ##### Other exposed implementation specific methods #####
  def get_topology_pex_path(self):
    """Returns the topology's pex file path"""
    return self.topology_pex_path

  def get_metrics_collector(self):
    """Returns this context's metrics collector"""
    if self.metrics_collector is None or not isinstance(self.metrics_collector, MetricsCollector):
      raise RuntimeError("Metrics collector is not registered in this context")
    return self.metrics_collector

  ########################################

  @classmethod
  def _get_inputs_and_outputs_and_outfields(cls, topology):
    inputs = {}
    outputs = {}
    out_fields = {}
    for spout in topology.spouts:
      inputs[spout.comp.name] = []  # spout doesn't have any inputs
      outputs[spout.comp.name] = spout.outputs
      out_fields.update(cls._get_output_to_comp_fields(spout.outputs))

    for bolt in topology.bolts:
      inputs[bolt.comp.name] = bolt.inputs
      outputs[bolt.comp.name] = bolt.outputs
      out_fields.update(cls._get_output_to_comp_fields(bolt.outputs))

    return inputs, outputs, out_fields

  @staticmethod
  def _get_output_to_comp_fields(outputs):
    out_fields = {}

    for out_stream in outputs:
      comp_name = out_stream.stream.component_name
      stream_id = out_stream.stream.id

      if comp_name not in out_fields:
        out_fields[comp_name] = dict()

      # get the fields of a particular output stream
      ret = []
      for kt in out_stream.schema.keys:
        ret.append(kt.key)

      out_fields[comp_name][stream_id] = tuple(ret)
    return out_fields

  ######### Task hook related ##########

  def _init_task_hooks(self):
    task_hooks_cls_list = self.get_cluster_config().get(api_constants.TOPOLOGY_AUTO_TASK_HOOKS,
                                                        None)
    if task_hooks_cls_list is None:
      return

    # load pex first
    topo_pex_path = self.get_topology_pex_path()
    pex_loader.load_pex(topo_pex_path)
    for class_name in task_hooks_cls_list:
      try:
        task_hook_cls = pex_loader.import_and_get_class(topo_pex_path, class_name)
        task_hook_instance = task_hook_cls()
        assert isinstance(task_hook_instance, ITaskHook)
        self.task_hooks.append(task_hook_instance)
      except AssertionError:
        raise RuntimeError("Auto-registered task hook not instance of ITaskHook")
      except Exception as e:
        raise RuntimeError("Error with loading task hook class: %s, with error message: %s"
                           % (class_name, str(e)))

  def invoke_hook_prepare(self):
    """invoke task hooks for after the spout/bolt's initialize() method"""
    for task_hook in self.task_hooks:
      task_hook.prepare(self.get_cluster_config(), self)

  def invoke_hook_cleanup(self):
    """invoke task hooks for just before the spout/bolt's cleanup method"""
    for task_hook in self.task_hooks:
      task_hook.clean_up()

  def invoke_hook_emit(self, values, stream_id, out_tasks):
    """invoke task hooks for every time a tuple is emitted in spout/bolt

    :type values: list
    :param values: values emitted
    :type stream_id: str
    :param stream_id: stream id into which tuple is emitted
    :type out_tasks: list
    :param out_tasks: list of custom grouping target task id
    """
    if len(self.task_hooks) > 0:
      emit_info = EmitInfo(values=values, stream_id=stream_id,
                           task_id=self.get_task_id(), out_tasks=out_tasks)
      for task_hook in self.task_hooks:
        task_hook.emit(emit_info)

  def invoke_hook_spout_ack(self, message_id, complete_latency_ns):
    """invoke task hooks for every time spout acks a tuple

    :type message_id: str
    :param message_id: message id to which an acked tuple was anchored
    :type complete_latency_ns: float
    :param complete_latency_ns: complete latency in nano seconds
    """
    if len(self.task_hooks) > 0:
      spout_ack_info = SpoutAckInfo(message_id=message_id,
                                    spout_task_id=self.get_task_id(),
                                    complete_latency_ms=complete_latency_ns *
                                    system_constants.NS_TO_MS)
      for task_hook in self.task_hooks:
        task_hook.spout_ack(spout_ack_info)

  def invoke_hook_spout_fail(self, message_id, fail_latency_ns):
    """invoke task hooks for every time spout fails a tuple

    :type message_id: str
    :param message_id: message id to which a failed tuple was anchored
    :type fail_latency_ns: float
    :param fail_latency_ns: fail latency in nano seconds
    """
    if len(self.task_hooks) > 0:
      spout_fail_info = SpoutFailInfo(message_id=message_id,
                                      spout_task_id=self.get_task_id(),
                                      fail_latency_ms=fail_latency_ns * system_constants.NS_TO_MS)
      for task_hook in self.task_hooks:
        task_hook.spout_fail(spout_fail_info)

  def invoke_hook_bolt_execute(self, heron_tuple, execute_latency_ns):
    """invoke task hooks for every time bolt processes a tuple

    :type heron_tuple: HeronTuple
    :param heron_tuple: tuple that is executed
    :type execute_latency_ns: float
    :param execute_latency_ns: execute latency in nano seconds
    """
    if len(self.task_hooks) > 0:
      bolt_execute_info = \
        BoltExecuteInfo(heron_tuple=heron_tuple,
                        executing_task_id=self.get_task_id(),
                        execute_latency_ms=execute_latency_ns * system_constants.NS_TO_MS)
      for task_hook in self.task_hooks:
        task_hook.bolt_execute(bolt_execute_info)

  def invoke_hook_bolt_ack(self, heron_tuple, process_latency_ns):
    """invoke task hooks for every time bolt acks a tuple

    :type heron_tuple: HeronTuple
    :param heron_tuple: tuple that is acked
    :type process_latency_ns: float
    :param process_latency_ns: process latency in nano seconds
    """
    if len(self.task_hooks) > 0:
      bolt_ack_info = BoltAckInfo(heron_tuple=heron_tuple,
                                  acking_task_id=self.get_task_id(),
                                  process_latency_ms=process_latency_ns * system_constants.NS_TO_MS)
      for task_hook in self.task_hooks:
        task_hook.bolt_ack(bolt_ack_info)

  def invoke_hook_bolt_fail(self, heron_tuple, fail_latency_ns):
    """invoke task hooks for every time bolt fails a tuple

    :type heron_tuple: HeronTuple
    :param heron_tuple: tuple that is failed
    :type fail_latency_ns: float
    :param fail_latency_ns: fail latency in nano seconds
    """
    if len(self.task_hooks) > 0:
      bolt_fail_info = BoltFailInfo(heron_tuple=heron_tuple,
                                    failing_task_id=self.get_task_id(),
                                    fail_latency_ms=fail_latency_ns * system_constants.NS_TO_MS)
      for task_hook in self.task_hooks:
        task_hook.bolt_fail(bolt_fail_info)
