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
'''topology_context.py'''
import os
from collections import namedtuple

from heron.common.src.python.utils.metrics import MetricsCollector

import heron.common.src.python.constants as constants
import heron.common.src.python.pex_loader as pex_loader

from .task_hook import (ITaskHook, EmitInfo, SpoutAckInfo, SpoutFailInfo, BoltExecuteInfo,
                        BoltAckInfo, BoltFailInfo)

class TopologyContext(dict):
  """Helper Class for Topology Context, inheriting from dict

  This is automatically created by Heron Instance and topology writers never need to create
  an instance by themselves.

  The following keys are present by default.

  :key CONFIG: contains cluster configuration
               (topology-wide config overriden by component-specific config)
  :key TOPOLOGY: contains Topology protobuf message
  :key TASK_TO_COMPONENT_MAP: contains dictionary mapping from task_id to component-id
  :key TASK_ID: contains task id for this component
  :key INPUTS: contains dictionary mapping from component_id to list of its inputs
               as protobuf InputStream messages
  :key OUTPUTS: contains dictionary mapping from component_id to list of its outputs
                as protobuf OutputStream messages
  :key COMPONENT_TO_OUT_FIELDS: contains nested dictionary mapping from component_id to
                                map <stream_id -> a list of output fields>
  :key TASK_HOOKS: list of registered ITaskHook classes
  :key METRICS_COLLECTOR: contains MetricsCollector object that is responsible for this component
  :key TOPOLOGY_PEX_PATH: contains the absolute path to the topology PEX file
  """
  # topology as supplied by the cluster overloaded by any component specific config
  CONFIG = 'config'
  # topology protobuf
  TOPOLOGY = 'topology'
  # dict <task_id -> component_id>
  TASK_TO_COMPONENT_MAP = 'task_to_component'
  # my task_id
  TASK_ID = 'task_id'
  # dict <component_id -> list of its inputs>
  INPUTS = 'inputs'
  # dict <component_id -> list of its outputs>
  OUTPUTS = 'outputs'
  # dict <component_id -> <stream_id -> fields>>
  COMPONENT_TO_OUT_FIELDS = 'comp_to_out_fields'
  # list of ITaskHook
  TASK_HOOKS = 'task_hooks'
  # path to topology pex file
  TOPOLOGY_PEX_PATH = 'topology_pex_path'

  METRICS_COLLECTOR = 'metrics_collector'

  def __init__(self, config, topology, task_to_component, my_task_id, metrics_collector,
               topo_pex_path, **kwargs):
    super(TopologyContext, self).__init__(**kwargs)
    self[self.CONFIG] = config
    self[self.TOPOLOGY] = topology
    self[self.TASK_TO_COMPONENT_MAP] = task_to_component
    self[self.TASK_ID] = my_task_id
    self[self.METRICS_COLLECTOR] = metrics_collector
    self[self.TOPOLOGY_PEX_PATH] = os.path.abspath(topo_pex_path)

    inputs, outputs, out_fields = self._get_inputs_and_outputs_and_outfields(topology)
    self[self.INPUTS] = inputs
    self[self.OUTPUTS] = outputs
    self[self.COMPONENT_TO_OUT_FIELDS] = out_fields

    # init task hooks
    self[self.TASK_HOOKS] = []
    self._init_task_hooks()

  ##### Helper method for common use #####

  @property
  def task_id(self):
    """Property to get the task id of this component"""
    return self[self.TASK_ID]

  @property
  def component_id(self):
    """Property to get the component id of this component"""
    return self[self.TASK_TO_COMPONENT_MAP].get(self.task_id)

  def get_cluster_config(self):
    """Returns the cluster config for this component

    Note that the returned config is auto-typed map: <str -> any Python object>.
    """
    return self[self.CONFIG]

  def get_topology_pex_path(self):
    """Returns the topology's pex file path"""
    return self[self.TOPOLOGY_PEX_PATH]

  def get_metrics_collector(self):
    """Returns this context's metrics collector"""
    if TopologyContext.METRICS_COLLECTOR not in self or \
        not isinstance(self.get(TopologyContext.METRICS_COLLECTOR), MetricsCollector):
      raise RuntimeError("Metrics collector is not registered in this context")
    return self.get(TopologyContext.METRICS_COLLECTOR)

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
    if component_id in self[self.INPUTS]:
      ret = {}
      for istream in self[self.INPUTS].get(component_id):
        key = StreamId(id=istream.stream.id, component_name=istream.stream.component_name)
        ret[key] = istream.gtype
      return ret
    else:
      return None

  def get_this_sources(self):
    return self.get_sources(self.component_id)

  def get_component_tasks(self, component_id):
    """Returns the task ids allocated for the given component id"""
    ret = []
    for task_id, comp_id in self[self.TASK_TO_COMPONENT_MAP].iteritems():
      if comp_id == component_id:
        ret.append(task_id)
    return ret

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
    task_hooks_cls_list = self.get_cluster_config().get(constants.TOPOLOGY_AUTO_TASK_HOOKS, None)
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
        self[self.TASK_HOOKS].append(task_hook_instance)
      except AssertionError:
        raise RuntimeError("Auto-registered task hook not instance of ITaskHook")
      except Exception as e:
        raise RuntimeError("Error with loading task hook class: %s, with error message: %s"
                           % (class_name, e.message))

  def add_task_hook(self, task_hook):
    """Registers a specified task hook to this context

    :type task_hook: heron.common.src.python.utils.topology.ITaskHook
    :param task_hook: Implementation of ITaskHook
    """
    if not isinstance(task_hook, ITaskHook):
      raise TypeError("In add_task_hook(): attempt to add non ITaskHook instance, given: %s"
                      % str(type(task_hook)))
    self[self.TASK_HOOKS].append(task_hook)

  @property
  def hook_exists(self):
    """Returns whether Task Hook is registered"""
    return len(self[self.TASK_HOOKS]) != 0

  def invoke_hook_prepare(self):
    """invoke task hooks for after the spout/bolt's initialize() method"""
    for task_hook in self[self.TASK_HOOKS]:
      task_hook.prepare(self.get_cluster_config(), self)

  def invoke_hook_cleanup(self):
    """invoke task hooks for just before the spout/bolt's cleanup method"""
    for task_hook in self[self.TASK_HOOKS]:
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
    if self.hook_exists:
      emit_info = EmitInfo(values=values, stream_id=stream_id,
                           task_id=self.task_id, out_tasks=out_tasks)
      for task_hook in self[self.TASK_HOOKS]:
        task_hook.emit(emit_info)

  def invoke_hook_spout_ack(self, message_id, complete_latency_ns):
    """invoke task hooks for every time spout acks a tuple

    :type message_id: str
    :param message_id: message id to which an acked tuple was anchored
    :type complete_latency_ns: float
    :param complete_latency_ns: complete latency in nano seconds
    """
    if self.hook_exists:
      spout_ack_info = SpoutAckInfo(message_id=message_id,
                                    spout_task_id=self.task_id,
                                    complete_latency_ms=complete_latency_ns * constants.NS_TO_MS)
      for task_hook in self[self.TASK_HOOKS]:
        task_hook.spout_ack(spout_ack_info)

  def invoke_hook_spout_fail(self, message_id, fail_latency_ns):
    """invoke task hooks for every time spout fails a tuple

    :type message_id: str
    :param message_id: message id to which a failed tuple was anchored
    :type fail_latency_ns: float
    :param fail_latency_ns: fail latency in nano seconds
    """
    if self.hook_exists:
      spout_fail_info = SpoutFailInfo(message_id=message_id,
                                      spout_task_id=self.task_id,
                                      fail_latency_ms=fail_latency_ns * constants.NS_TO_MS)
      for task_hook in self[self.TASK_HOOKS]:
        task_hook.spout_fail(spout_fail_info)

  def invoke_hook_bolt_execute(self, heron_tuple, execute_latency_ns):
    """invoke task hooks for every time bolt processes a tuple

    :type heron_tuple: HeronTuple
    :param heron_tuple: tuple that is executed
    :type execute_latency_ns: float
    :param execute_latency_ns: execute latency in nano seconds
    """
    if self.hook_exists:
      bolt_execute_info = \
        BoltExecuteInfo(heron_tuple=heron_tuple,
                        executing_task_id=self.task_id,
                        execute_latency_ms=execute_latency_ns * constants.NS_TO_MS)
      for task_hook in self[self.TASK_HOOKS]:
        task_hook.bolt_execute(bolt_execute_info)

  def invoke_hook_bolt_ack(self, heron_tuple, process_latency_ns):
    """invoke task hooks for every time bolt acks a tuple

    :type heron_tuple: HeronTuple
    :param heron_tuple: tuple that is acked
    :type process_latency_ns: float
    :param process_latency_ns: process latency in nano seconds
    """
    if self.hook_exists:
      bolt_ack_info = BoltAckInfo(heron_tuple=heron_tuple,
                                  acking_task_id=self.task_id,
                                  process_latency_ms=process_latency_ns * constants.NS_TO_MS)
      for task_hook in self[self.TASK_HOOKS]:
        task_hook.bolt_ack(bolt_ack_info)

  def invoke_hook_bolt_fail(self, heron_tuple, fail_latency_ns):
    """invoke task hooks for every time bolt fails a tuple

    :type heron_tuple: HeronTuple
    :param heron_tuple: tuple that is failed
    :type fail_latency_ns: float
    :param fail_latency_ns: fail latency in nano seconds
    """
    if self.hook_exists:
      bolt_fail_info = BoltFailInfo(heron_tuple=heron_tuple,
                                    failing_task_id=self.task_id,
                                    fail_latency_ms=fail_latency_ns * constants.NS_TO_MS)
      for task_hook in self[self.TASK_HOOKS]:
        task_hook.bolt_fail(bolt_fail_info)
