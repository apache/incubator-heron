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

'''spout_instance.py: module for base spout for python topology'''

import queue
import time
import collections

from heronpy.api.stream import Stream
import heronpy.api.api_constants as api_constants
from heronpy.api.state.stateful_component import StatefulComponent

from heron.common.src.python.utils.log import Log

from heron.instance.src.python.utils.metrics import SpoutMetrics
from heron.instance.src.python.utils.tuple import TupleHelper

from heron.proto import topology_pb2, tuple_pb2, ckptmgr_pb2

import heron.instance.src.python.utils.system_constants as system_constants

from .base_instance import BaseInstance

# pylint: disable=too-many-instance-attributes
class SpoutInstance(BaseInstance):
  """The base class for all heron spouts in Python"""

  def __init__(self, pplan_helper, in_stream, out_stream, looper):
    super(SpoutInstance, self).__init__(pplan_helper, in_stream, out_stream, looper)
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

    if not self.pplan_helper.is_spout:
      raise RuntimeError("No spout in physicial plan")

    context = self.pplan_helper.context
    self.spout_metrics = SpoutMetrics(self.pplan_helper)

    # acking related
    mode = context.get_cluster_config().get(api_constants.TOPOLOGY_RELIABILITY_MODE,
                                            api_constants.TopologyReliabilityMode.ATMOST_ONCE)
    self.acking_enabled = bool(mode == api_constants.TopologyReliabilityMode.ATLEAST_ONCE)
    self.enable_message_timeouts = \
      context.get_cluster_config().get(api_constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS)
    self._initialized_metrics_and_tasks = False
    Log.info("Enable ACK: %s" % str(self.acking_enabled))
    Log.info("Enable Message Timeouts: %s" % str(self.enable_message_timeouts))

    # map <tuple_info.key -> tuple_info>, ordered by insertion time
    self.in_flight_tuples = collections.OrderedDict()
    self.immediate_acks = collections.deque()
    self.total_tuples_emitted = 0

    # load user's spout class
    spout_impl_class = super(SpoutInstance, self).load_py_instance(is_spout=True)
    self.spout_impl = spout_impl_class(delegate=self)

  def start_component(self, stateful_state):
    context = self.pplan_helper.context
    if not self._initialized_metrics_and_tasks:
      self.spout_metrics.register_metrics(context)
    if self.is_stateful and isinstance(self.spout_impl, StatefulComponent):
      self.spout_impl.init_state(stateful_state)
    self.spout_impl.initialize(config=context.get_cluster_config(), context=context)
    if not self._initialized_metrics_and_tasks:
      self._add_spout_task()
    self._initialized_metrics_and_tasks = True
    self.topology_state = topology_pb2.TopologyState.Value("RUNNING")

  def stop_component(self):
    self.spout_impl.close()
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

  def invoke_activate(self):
    Log.info("Spout is activated")
    self.spout_impl.activate()
    self.topology_state = topology_pb2.TopologyState.Value("RUNNING")

  def invoke_deactivate(self):
    Log.info("Spout is deactivated")
    self.spout_impl.deactivate()
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

  def emit(self, tup, tup_id=None, stream=Stream.DEFAULT_STREAM_ID,
           direct_task=None, need_task_ids=False):
    """Emits a new tuple from this Spout

    It is compatible with StreamParse API.

    :type tup: list or tuple
    :param tup: the new output Tuple to send from this spout,
                should contain only serializable data.
    :type tup_id: str or object
    :param tup_id: the ID for the Tuple. Leave this blank for an unreliable emit.
                   (Same as messageId in Java)
    :type stream: str
    :param stream: the ID of the stream this Tuple should be emitted to.
                   Leave empty to emit to the default stream.
    :type direct_task: int
    :param direct_task: the task to send the Tuple to if performing a direct emit.
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
      # for custom grouping
      for task_id in custom_target_task_ids:
        data_tuple.dest_task_ids.append(task_id)

    if tup_id is not None:
      tuple_info = TupleHelper.make_root_tuple_info(stream, tup_id)
      if self.acking_enabled:
        # this message is rooted
        root = data_tuple.roots.add()
        root.taskid = self.pplan_helper.my_task_id
        root.key = tuple_info.key
        self.in_flight_tuples[tuple_info.key] = tuple_info
      else:
        self.immediate_acks.append(tuple_info)

    tuple_size_in_bytes = 0

    start_time = time.time()

    # Serialize
    for obj in tup:
      serialized = self.serializer.serialize(obj)
      data_tuple.values.append(serialized)
      tuple_size_in_bytes += len(serialized)

    serialize_latency_ns = (time.time() - start_time) * system_constants.SEC_TO_NS
    self.spout_metrics.serialize_data_tuple(stream, serialize_latency_ns)

    super(SpoutInstance, self).admit_data_tuple(stream_id=stream, data_tuple=data_tuple,
                                                tuple_size_in_bytes=tuple_size_in_bytes)
    self.total_tuples_emitted += 1
    self.spout_metrics.update_emit_count(stream)
    if need_task_ids:
      sent_task_ids = custom_target_task_ids or []
      if direct_task is not None:
        sent_task_ids.append(direct_task)
      return sent_task_ids

  # pylint: disable=no-self-use
  def process_incoming_tuples(self):
    self.looper.wake_up()

  def _read_tuples(self):
    start_cycle_time = time.time()
    ack_batch_time = self.sys_config[system_constants.INSTANCE_ACK_BATCH_TIME_MS] * \
                     system_constants.MS_TO_SEC
    while not self.in_stream.is_empty():
      try:
        tuples = self.in_stream.poll()
      except queue.Empty:
        break

      if isinstance(tuples, tuple_pb2.HeronTupleSet):
        if tuples.HasField("data"):
          raise RuntimeError("Spout cannot get incoming data tuples from other components")
        elif tuples.HasField("control"):
          for ack_tuple in tuples.control.acks:
            self._handle_ack_tuple(ack_tuple, True)
          for fail_tuple in tuples.control.fails:
            self._handle_ack_tuple(fail_tuple, False)
        else:
          Log.error("Received tuple neither data nor control")
      elif isinstance(tuples, ckptmgr_pb2.InitiateStatefulCheckpoint):
        self.handle_initiate_stateful_checkpoint(tuples, self.spout_impl)
      else:
        Log.error("Received tuple not instance of HeronTupleSet")

      # avoid spending too much time here
      if time.time() - start_cycle_time - ack_batch_time > 0:
        break

  def _produce_tuple(self):
    # TOPOLOGY_MAX_SPOUT_PENDING must be provided (if not included, raise KeyError)
    max_spout_pending = \
      self.pplan_helper.context.get_cluster_config().get(api_constants.TOPOLOGY_MAX_SPOUT_PENDING)

    total_tuples_emitted_before = self.total_tuples_emitted
    total_data_emitted_bytes_before = self.get_total_data_emitted_in_bytes()
    emit_batch_time = \
      float(self.sys_config[system_constants.INSTANCE_EMIT_BATCH_TIME_MS]) * \
      system_constants.MS_TO_SEC
    emit_batch_size = int(self.sys_config[system_constants.INSTANCE_EMIT_BATCH_SIZE_BYTES])
    start_cycle_time = time.time()

    while (self.acking_enabled and max_spout_pending > len(self.in_flight_tuples)) or \
        not self.acking_enabled:
      start_time = time.time()
      self.spout_impl.next_tuple()
      next_tuple_latency_ns = (time.time() - start_time) * system_constants.SEC_TO_NS
      self.spout_metrics.next_tuple(next_tuple_latency_ns)

      if (self.total_tuples_emitted == total_tuples_emitted_before) or \
        (time.time() - start_cycle_time - emit_batch_time > 0) or \
        (self.get_total_data_emitted_in_bytes() - total_data_emitted_bytes_before >
         emit_batch_size):
        # no tuples to emit or batch reached
        break

      total_tuples_emitted_before = self.total_tuples_emitted

  def _add_spout_task(self):
    Log.info("Adding spout task...")
    def spout_task():
      # don't do anything when topology is paused
      if not self._is_topology_running():
        return

      self._read_tuples()

      if self._should_produce_tuple():
        self._produce_tuple()
        self.output_helper.send_out_tuples()
        self.looper.wake_up() # so emitted tuples would be added to buffer now
      else:
        self.spout_metrics.update_out_queue_full_count()

      if self.acking_enabled:
        self.spout_metrics.update_pending_tuples_count(len(self.in_flight_tuples))
      else:
        self._do_immediate_acks()

      if self._is_continue_to_work():
        self.looper.wake_up()

    self.looper.add_wakeup_task(spout_task)

    # look for the timeout's tuples
    if self.enable_message_timeouts:
      self._look_for_timeouts()

  def _is_topology_running(self):
    return self.topology_state == topology_pb2.TopologyState.Value("RUNNING")

  def _should_produce_tuple(self):
    """Checks whether we could produce tuples, i.e. invoke spout.next_tuple()"""
    return self._is_topology_running() and self.output_helper.is_out_queue_available()

  def _is_continue_to_work(self):
    """Checks whether we still need to do more work

    When the topology state is RUNNING:
    1. if the out_queue is not full and ack is not enabled, we could wake up next time to
       produce more tuples and push to the out_queue
    2. if the out_queue is not full but the acking is enabled, we need to make sure that
      the number of pending tuples is smaller than max_spout_pending
    3. if there are more to read, we will wake up itself next time.
    """
    if not self._is_topology_running():
      return False

    max_spout_pending = \
      self.pplan_helper.context.get_cluster_config().get(api_constants.TOPOLOGY_MAX_SPOUT_PENDING)

    if not self.acking_enabled and self.output_helper.is_out_queue_available():
      return True
    elif self.acking_enabled and self.output_helper.is_out_queue_available() and \
        len(self.in_flight_tuples) < max_spout_pending:
      return True
    elif self.acking_enabled and not self.in_stream.is_empty():
      return True
    else:
      return False

  def _look_for_timeouts(self):
    spout_config = self.pplan_helper.context.get_cluster_config()
    timeout_sec = spout_config.get(api_constants.TOPOLOGY_MESSAGE_TIMEOUT_SECS)
    n_bucket = self.sys_config.get(system_constants.INSTANCE_ACKNOWLEDGEMENT_NBUCKETS)
    now = time.time()

    timeout_lst = []
    while self.in_flight_tuples:
      key = next(iter(self.in_flight_tuples))
      tuple_info = self.in_flight_tuples[key]
      if tuple_info.is_expired(now, timeout_sec):
        timeout_lst.append(tuple_info)
        self.in_flight_tuples.pop(key)
      else:
        # in_flight_tuples are ordered by insertion time
        break

    for tuple_info in timeout_lst:
      self.spout_metrics.timeout_tuple(tuple_info.stream_id)
      self._invoke_fail(tuple_info.tuple_id, tuple_info.stream_id,
                        timeout_sec * system_constants.SEC_TO_NS)

    # register this method to timer again
    self.looper.register_timer_task_in_sec(self._look_for_timeouts, float(timeout_sec) / n_bucket)

  # ACK/FAIL related
  def _handle_ack_tuple(self, tup, is_success):
    for rt in tup.roots:
      if rt.taskid != self.pplan_helper.my_task_id:
        raise RuntimeError("Receiving tuple for task: %s in task: %s"
                           % (str(rt.taskid), str(self.pplan_helper.my_task_id)))
      try:
        tuple_info = self.in_flight_tuples.pop(rt.key)
      except KeyError:
        # rt.key is not in in_flight_tuples -> already removed due to time-out
        return

      # pylint: disable=no-member
      if tuple_info.tuple_id is not None:
        latency_ns = (time.time() - tuple_info.insertion_time) * system_constants.SEC_TO_NS
        if is_success:
          self._invoke_ack(tuple_info.tuple_id, tuple_info.stream_id, latency_ns)
        else:
          self._invoke_fail(tuple_info.tuple_id, tuple_info.stream_id, latency_ns)

  def _do_immediate_acks(self):
    size = len(self.immediate_acks)
    for _ in range(size):
      tuple_info = self.immediate_acks.pop()
      self._invoke_ack(tuple_info.tuple_id, tuple_info.stream_id, 0)

  def _invoke_ack(self, tuple_id, stream_id, complete_latency_ns):
    Log.debug("In invoke_ack(): Acking %s from stream: %s" % (str(tuple_id), stream_id))
    self.spout_impl.ack(tuple_id)
    self.pplan_helper.context.invoke_hook_spout_ack(tuple_id, complete_latency_ns)
    self.spout_metrics.acked_tuple(stream_id, complete_latency_ns)

  def _invoke_fail(self, tuple_id, stream_id, fail_latency_ns):
    Log.debug("In invoke_fail(): Failing %s from stream: %s" % (str(tuple_id), stream_id))
    self.spout_impl.fail(tuple_id)
    self.pplan_helper.context.invoke_hook_spout_fail(tuple_id, fail_latency_ns)
    self.spout_metrics.failed_tuple(stream_id, fail_latency_ns)
