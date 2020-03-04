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

'''bolt_instance.py: module for base bolt for python topology'''

import time
import queue

import heronpy.api.api_constants as api_constants
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.stream import Stream

from heron.common.src.python.utils.log import Log

from heron.proto import topology_pb2, tuple_pb2, ckptmgr_pb2

from heron.instance.src.python.utils.metrics import BoltMetrics
from heron.instance.src.python.utils.tuple import TupleHelper, HeronTuple

import heron.instance.src.python.utils.system_constants as system_constants

from .base_instance import BaseInstance

class BoltInstance(BaseInstance):
  """The base class for all heron bolts in Python"""

  def __init__(self, pplan_helper, in_stream, out_stream, looper):
    super(BoltInstance, self).__init__(pplan_helper, in_stream, out_stream, looper)
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

    if self.pplan_helper.is_spout:
      raise RuntimeError("No bolt in physical plan")

    # bolt_config is auto-typed, not <str -> str> only
    context = self.pplan_helper.context
    self.bolt_metrics = BoltMetrics(self.pplan_helper)

    # acking related
    mode = context.get_cluster_config().get(api_constants.TOPOLOGY_RELIABILITY_MODE,
                                            api_constants.TopologyReliabilityMode.ATMOST_ONCE)
    self.acking_enabled = bool(mode == api_constants.TopologyReliabilityMode.ATLEAST_ONCE)
    self._initialized_metrics_and_tasks = False
    Log.info("Enable ACK: %s" % str(self.acking_enabled))

    # load user's bolt class
    bolt_impl_class = super(BoltInstance, self).load_py_instance(is_spout=False)
    self.bolt_impl = bolt_impl_class(delegate=self)

  def start_component(self, stateful_state):
    context = self.pplan_helper.context
    if not self._initialized_metrics_and_tasks:
      self.bolt_metrics.register_metrics(context)
    if self.is_stateful and isinstance(self.bolt_impl, StatefulComponent):
      self.bolt_impl.init_state(stateful_state)
    self.bolt_impl.initialize(config=context.get_cluster_config(), context=context)
    # prepare tick tuple
    if not self._initialized_metrics_and_tasks:
      self._prepare_tick_tup_timer()
    self._initialized_metrics_and_tasks = True
    self.topology_state = topology_pb2.TopologyState.Value("RUNNING")

  def stop_component(self):
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

  def invoke_activate(self):
    Log.info("Activating Bolt")
    self.topology_state = topology_pb2.TopologyState.Value("RUNNING")

  def invoke_deactivate(self):
    Log.info("Deactivating Bolt")
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

  def emit(self, tup, stream=Stream.DEFAULT_STREAM_ID,
           anchors=None, direct_task=None, need_task_ids=False):
    """Emits a new tuple from this Bolt

    It is compatible with StreamParse API.

    :type tup: list or tuple
    :param tup: the new output Tuple to send from this bolt,
                should only contain only serializable data.
    :type stream: str
    :param stream: the ID of the stream to emit this Tuple to.
                   Leave empty to emit to the default stream.
    :type anchors: list
    :param anchors: a list of HeronTuples to which the emitted Tuples should be anchored.
    :type direct_task: int
    :param direct_task: the task to send the Tupel to if performing a direct emit.
    :type need_task_ids: bool
    :param need_task_ids: indicate whether or not you would like the task IDs the Tuple was emitted.
    """
    # first check whether this tuple is sane
    self.pplan_helper.check_output_schema(stream, tup)

    # get custom grouping target task ids; get empty list if not custom grouping
    custom_target_task_ids = self.pplan_helper.choose_tasks_for_custom_grouping(stream, tup)

    self.pplan_helper.context.invoke_hook_emit(tup, stream, None)

    data_tuple = tuple_pb2.HeronDataTuple()
    data_tuple.key = 0

    if direct_task is not None:
      if not isinstance(direct_task, int):
        raise TypeError("direct_task argument needs to be an integer, given: %s"
                        % str(type(direct_task)))
      # performing emit-direct
      data_tuple.dest_task_ids.append(direct_task)
    elif custom_target_task_ids is not None:
      for task_id in custom_target_task_ids:
        # for custom grouping
        data_tuple.dest_task_ids.append(task_id)

    # Set the anchors for a tuple
    if anchors is not None:
      merged_roots = set()
      for tup in [t for t in anchors if isinstance(t, HeronTuple) and t.roots is not None]:
        merged_roots.update(tup.roots)
      for rt in merged_roots:
        to_add = data_tuple.roots.add()
        to_add.CopyFrom(rt)

    tuple_size_in_bytes = 0
    start_time = time.time()

    # Serialize
    for obj in tup:
      serialized = self.serializer.serialize(obj)
      data_tuple.values.append(serialized)
      tuple_size_in_bytes += len(serialized)
    serialize_latency_ns = (time.time() - start_time) * system_constants.SEC_TO_NS
    self.bolt_metrics.serialize_data_tuple(stream, serialize_latency_ns)

    super(BoltInstance, self).admit_data_tuple(stream_id=stream, data_tuple=data_tuple,
                                               tuple_size_in_bytes=tuple_size_in_bytes)

    self.bolt_metrics.update_emit_count(stream)
    if need_task_ids:
      sent_task_ids = custom_target_task_ids or []
      if direct_task is not None:
        sent_task_ids.append(direct_task)
      return sent_task_ids

  def process_incoming_tuples(self):
    """Should be called when tuple was buffered into in_stream

    This method is equivalent to ``addBoltTasks()`` but
    is designed for event-driven single-thread bolt.
    """
    # back-pressure
    if self.output_helper.is_out_queue_available():
      self._read_tuples_and_execute()
      self.output_helper.send_out_tuples()
    else:
      # update outqueue full count
      self.bolt_metrics.update_out_queue_full_count()

  def _read_tuples_and_execute(self):
    start_cycle_time = time.time()
    total_data_emitted_bytes_before = self.get_total_data_emitted_in_bytes()
    exec_batch_time = \
      self.sys_config[system_constants.INSTANCE_EXECUTE_BATCH_TIME_MS] * system_constants.MS_TO_SEC
    exec_batch_size = self.sys_config[system_constants.INSTANCE_EXECUTE_BATCH_SIZE_BYTES]
    while not self.in_stream.is_empty():
      try:
        tuples = self.in_stream.poll()
      except queue.Empty:
        break

      if isinstance(tuples, tuple_pb2.HeronTupleSet):
        if tuples.HasField("control"):
          raise RuntimeError("Bolt cannot get acks/fails from other components")
        elif tuples.HasField("data"):
          stream = tuples.data.stream

          for data_tuple in tuples.data.tuples:
            self._handle_data_tuple(data_tuple, stream)
        else:
          Log.error("Received tuple neither data nor control")
      elif isinstance(tuples, ckptmgr_pb2.InitiateStatefulCheckpoint):
        self.handle_initiate_stateful_checkpoint(tuples, self.bolt_impl)
      else:
        Log.error("Received tuple not instance of HeronTupleSet")

      if (time.time() - start_cycle_time - exec_batch_time > 0) or \
          (self.get_total_data_emitted_in_bytes() - total_data_emitted_bytes_before
           > exec_batch_size):
        # batch reached
        break

  def _handle_data_tuple(self, data_tuple, stream):
    start_time = time.time()

    values = []
    for value in data_tuple.values:
      values.append(self.serializer.deserialize(value))

    # create HeronTuple
    tup = TupleHelper.make_tuple(stream, data_tuple.key, values, roots=data_tuple.roots)

    deserialized_time = time.time()
    self.bolt_impl.process(tup)
    execute_latency_ns = (time.time() - deserialized_time) * system_constants.SEC_TO_NS
    deserialize_latency_ns = (deserialized_time - start_time) * system_constants.SEC_TO_NS

    self.pplan_helper.context.invoke_hook_bolt_execute(tup, execute_latency_ns)

    self.bolt_metrics.deserialize_data_tuple(stream.id, stream.component_name,
                                             deserialize_latency_ns)
    self.bolt_metrics.execute_tuple(stream.id, stream.component_name, execute_latency_ns)

  def _prepare_tick_tup_timer(self):
    cluster_config = self.pplan_helper.context.get_cluster_config()
    if api_constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS in cluster_config:
      tick_freq_sec = cluster_config[api_constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS]
      Log.debug("Tick Tuple Frequency: %s sec." % str(tick_freq_sec))

      def send_tick():
        tick = TupleHelper.make_tick_tuple()
        start_time = time.time()
        self.bolt_impl.process_tick(tick)
        tick_execute_latency_ns = (time.time() - start_time) * system_constants.SEC_TO_NS
        self.bolt_metrics.execute_tuple(tick.id, tick.component, tick_execute_latency_ns)
        self.output_helper.send_out_tuples()
        self.looper.wake_up() # so emitted tuples would be added to buffer now
        self._prepare_tick_tup_timer()

      self.looper.register_timer_task_in_sec(send_tick, tick_freq_sec)

  def ack(self, tup):
    """Indicate that processing of a Tuple has succeeded

    It is compatible with StreamParse API.
    """
    if not isinstance(tup, HeronTuple):
      Log.error("Only HeronTuple type is supported in ack()")
      return

    if self.acking_enabled:
      ack_tuple = tuple_pb2.AckTuple()
      ack_tuple.ackedtuple = int(tup.id)

      tuple_size_in_bytes = 0
      for rt in tup.roots:
        to_add = ack_tuple.roots.add()
        to_add.CopyFrom(rt)
        tuple_size_in_bytes += rt.ByteSize()
      super(BoltInstance, self).admit_control_tuple(ack_tuple, tuple_size_in_bytes, True)

    process_latency_ns = (time.time() - tup.creation_time) * system_constants.SEC_TO_NS
    self.pplan_helper.context.invoke_hook_bolt_ack(tup, process_latency_ns)
    self.bolt_metrics.acked_tuple(tup.stream, tup.component, process_latency_ns)

  def fail(self, tup):
    """Indicate that processing of a Tuple has failed

    It is compatible with StreamParse API.
    """
    if not isinstance(tup, HeronTuple):
      Log.error("Only HeronTuple type is supported in fail()")
      return

    if self.acking_enabled:
      fail_tuple = tuple_pb2.AckTuple()
      fail_tuple.ackedtuple = int(tup.id)

      tuple_size_in_bytes = 0
      for rt in tup.roots:
        to_add = fail_tuple.roots.add()
        to_add.CopyFrom(rt)
        tuple_size_in_bytes += rt.ByteSize()
      super(BoltInstance, self).admit_control_tuple(fail_tuple, tuple_size_in_bytes, False)

    fail_latency_ns = (time.time() - tup.creation_time) * system_constants.SEC_TO_NS
    self.pplan_helper.context.invoke_hook_bolt_fail(tup, fail_latency_ns)
    self.bolt_metrics.failed_tuple(tup.stream, tup.component, fail_latency_ns)
