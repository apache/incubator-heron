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

'''metrics_helper: helper classes for managing common metrics'''

from heron.common.src.python.utils.log import Log
import heron.instance.src.python.utils.system_constants as constants
from heron.instance.src.python.utils import system_config

from heron.proto import metrics_pb2

from heronpy.api.metrics import (CountMetric, MultiCountMetric, MeanReducedMetric,
                                 ReducedMetric, MultiMeanReducedMetric, MultiReducedMetric)

class BaseMetricsHelper(object):
  """Helper class for metrics management

  It registers metrics to the metrics collector and provides methods for
  updating metrics. This supports updating of: CountMetric, MultiCountMetric,
  ReducedMetric and MultiReducedMetric class.
  """
  def __init__(self, metrics):
    self.metrics = metrics

  def register_metrics(self, metrics_collector, interval):
    """Registers its metrics to a given metrics collector with a given interval"""
    for field, metrics in list(self.metrics.items()):
      metrics_collector.register_metric(field, metrics, interval)

  def update_count(self, name, incr_by=1, key=None):
    """Update the value of CountMetric or MultiCountMetric

    :type name: str
    :param name: name of the registered metric to be updated.
    :type incr_by: int
    :param incr_by: specifies how much to increment. Default is 1.
    :type key: str or None
    :param key: specifies a key for MultiCountMetric. Needs to be `None` for updating CountMetric.
    """
    if name not in self.metrics:
      Log.error("In update_count(): %s is not registered in the metric", name)

    if key is None and isinstance(self.metrics[name], CountMetric):
      self.metrics[name].incr(incr_by)
    elif key is not None and isinstance(self.metrics[name], MultiCountMetric):
      self.metrics[name].incr(key, incr_by)
    else:
      Log.error("In update_count(): %s is registered but not supported with this method", name)

  def update_reduced_metric(self, name, value, key=None):
    """Update the value of ReducedMetric or MultiReducedMetric

    :type name: str
    :param name: name of the registered metric to be updated.
    :param value: specifies a value to be reduced.
    :type key: str or None
    :param key: specifies a key for MultiReducedMetric. Needs to be `None` for updating
                ReducedMetric.
    """
    if name not in self.metrics:
      Log.error("In update_reduced_metric(): %s is not registered in the metric", name)

    if key is None and isinstance(self.metrics[name], ReducedMetric):
      self.metrics[name].update(value)
    elif key is not None and isinstance(self.metrics[name], MultiReducedMetric):
      self.metrics[name].update(key, value)
    else:
      Log.error("In update_count(): %s is registered but not supported with this method", name)


class GatewayMetrics(BaseMetricsHelper):
  """Metrics helper class for Gateway metric"""
  RECEIVED_PKT_SIZE = '__gateway-received-packets-size'
  SENT_PKT_SIZE = '__gateway-sent-packets-size'
  RECEIVED_PKT_COUNT = '__gateway-received-packets-count'
  SENT_PKT_COUNT = '__gateway-sent-packets-count'

  SENT_METRICS_SIZE = '__gateway-sent-metrics-size'
  SENT_METRICS_PKT_COUNT = '__gateway-sent-metrics-packets-count'
  SENT_METRICS_COUNT = '__gateway-sent-metrics-count'
  SENT_EXCEPTION_COUNT = '__gateway-sent-exceptions-count'

  IN_STREAM_QUEUE_SIZE = '__gateway-in-stream-queue-size'
  OUT_STREAM_QUEUE_SIZE = '__gateway-out-stream-queue-size'

  IN_STREAM_QUEUE_EXPECTED_CAPACITY = '__gateway-in-stream-queue-expected-capacity'
  OUT_STREAM_QUEUE_EXPECTED_CAPACITY = '__gateway-out-stream-queue-expected-capacity'

  IN_QUEUE_FULL_COUNT = '__gateway-in-queue-full-count'

  metrics = {RECEIVED_PKT_SIZE: CountMetric(),
             SENT_PKT_SIZE: CountMetric(),
             RECEIVED_PKT_COUNT: CountMetric(),
             SENT_PKT_COUNT: CountMetric(),

             SENT_METRICS_SIZE: CountMetric(),
             SENT_METRICS_PKT_COUNT: CountMetric(),
             SENT_METRICS_COUNT: CountMetric(),
             SENT_EXCEPTION_COUNT: CountMetric(),

             IN_STREAM_QUEUE_SIZE: MeanReducedMetric(),
             OUT_STREAM_QUEUE_SIZE: MeanReducedMetric(),
             IN_STREAM_QUEUE_EXPECTED_CAPACITY: MeanReducedMetric(),
             OUT_STREAM_QUEUE_EXPECTED_CAPACITY: MeanReducedMetric()}

  def __init__(self, metrics_collector):
    sys_config = system_config.get_sys_config()
    super(GatewayMetrics, self).__init__(self.metrics)
    interval = float(sys_config[constants.HERON_METRICS_EXPORT_INTERVAL_SEC])
    self.register_metrics(metrics_collector, interval)

  def update_received_packet(self, received_pkt_size_bytes):
    """Update received packet metrics"""
    self.update_count(self.RECEIVED_PKT_COUNT)
    self.update_count(self.RECEIVED_PKT_SIZE, incr_by=received_pkt_size_bytes)

  def update_sent_packet(self, sent_pkt_size_bytes):
    """Update sent packet metrics"""
    self.update_count(self.SENT_PKT_COUNT)
    self.update_count(self.SENT_PKT_SIZE, incr_by=sent_pkt_size_bytes)

  def update_sent_metrics_size(self, size):
    self.update_count(self.SENT_METRICS_SIZE, size)

  def update_sent_metrics(self, metrics_count, exceptions_count):
    self.update_count(self.SENT_METRICS_PKT_COUNT)
    self.update_count(self.SENT_METRICS_COUNT, metrics_count)
    self.update_count(self.SENT_EXCEPTION_COUNT, exceptions_count)

  def update_in_out_stream_metrics(self, in_size, out_size, in_expect_size, out_expect_size):
    self.update_reduced_metric(self.IN_STREAM_QUEUE_SIZE, in_size)
    self.update_reduced_metric(self.OUT_STREAM_QUEUE_SIZE, out_size)
    self.update_reduced_metric(self.IN_STREAM_QUEUE_EXPECTED_CAPACITY, in_expect_size)
    self.update_reduced_metric(self.OUT_STREAM_QUEUE_EXPECTED_CAPACITY, out_expect_size)


class ComponentMetrics(BaseMetricsHelper):
  """Metrics to be collected for both Bolt and Spout"""
  FAIL_LATENCY = "__fail-latency"
  FAIL_COUNT = "__fail-count"
  EMIT_COUNT = "__emit-count"
  TUPLE_SERIALIZATION_TIME_NS = "__tuple-serialization-time-ns"
  OUT_QUEUE_FULL_COUNT = "__out-queue-full-count"

  component_metrics = {FAIL_LATENCY: MultiMeanReducedMetric(),
                       FAIL_COUNT: MultiCountMetric(),
                       EMIT_COUNT: MultiCountMetric(),
                       TUPLE_SERIALIZATION_TIME_NS: MultiCountMetric(),
                       OUT_QUEUE_FULL_COUNT: CountMetric()}

  def __init__(self, additional_metrics):
    metrics = self.component_metrics
    metrics.update(additional_metrics)
    super(ComponentMetrics, self).__init__(metrics)

  # pylint: disable=arguments-differ
  def register_metrics(self, context):
    """Registers metrics to context

    :param context: Topology Context
    """
    sys_config = system_config.get_sys_config()
    interval = float(sys_config[constants.HERON_METRICS_EXPORT_INTERVAL_SEC])
    collector = context.get_metrics_collector()
    super(ComponentMetrics, self).register_metrics(collector, interval)

  def update_out_queue_full_count(self):
    """Apply update to the out-queue full count"""
    self.update_count(self.OUT_QUEUE_FULL_COUNT)

  def update_emit_count(self, stream_id):
    """Apply update to emit count"""
    self.update_count(self.EMIT_COUNT, key=stream_id)

  def serialize_data_tuple(self, stream_id, latency_in_ns):
    """Apply update to serialization metrics"""
    self.update_count(self.TUPLE_SERIALIZATION_TIME_NS, incr_by=latency_in_ns, key=stream_id)

class SpoutMetrics(ComponentMetrics):
  """Metrics helper class for Spout"""
  ACK_COUNT = "__ack-count"
  COMPLETE_LATENCY = "__complete-latency"
  TIMEOUT_COUNT = "__timeout-count"
  NEXT_TUPLE_LATENCY = "__next-tuple-latency"
  NEXT_TUPLE_COUNT = "__next-tuple-count"
  PENDING_ACKED_COUNT = "__pending-acked-count"

  spout_metrics = {ACK_COUNT: MultiCountMetric(),
                   COMPLETE_LATENCY: MultiMeanReducedMetric(),
                   TIMEOUT_COUNT: MultiCountMetric(),
                   NEXT_TUPLE_LATENCY: MeanReducedMetric(),
                   NEXT_TUPLE_COUNT: CountMetric(),
                   PENDING_ACKED_COUNT: MeanReducedMetric()}

  to_multi_init = [ACK_COUNT, ComponentMetrics.FAIL_COUNT,
                   TIMEOUT_COUNT, ComponentMetrics.EMIT_COUNT]

  def __init__(self, pplan_helper):
    super(SpoutMetrics, self).__init__(self.spout_metrics)
    self._init_multi_count_metrics(pplan_helper)

  def _init_multi_count_metrics(self, pplan_helper):
    """Initializes the default values for a necessary set of MultiCountMetrics"""
    to_init = [self.metrics[i] for i in self.to_multi_init
               if i in self.metrics and isinstance(self.metrics[i], MultiCountMetric)]
    for out_stream in pplan_helper.get_my_spout().outputs:
      stream_id = out_stream.stream.id
      for metric in to_init:
        metric.add_key(stream_id)

  def next_tuple(self, latency_in_ns):
    """Apply updates to the next tuple metrics"""
    self.update_reduced_metric(self.NEXT_TUPLE_LATENCY, latency_in_ns)
    self.update_count(self.NEXT_TUPLE_COUNT)

  def acked_tuple(self, stream_id, complete_latency_ns):
    """Apply updates to the ack metrics"""
    self.update_count(self.ACK_COUNT, key=stream_id)
    self.update_reduced_metric(self.COMPLETE_LATENCY, complete_latency_ns, key=stream_id)

  def failed_tuple(self, stream_id, fail_latency_ns):
    """Apply updates to the fail metrics"""
    self.update_count(self.FAIL_COUNT, key=stream_id)
    self.update_reduced_metric(self.FAIL_LATENCY, fail_latency_ns, key=stream_id)

  def update_pending_tuples_count(self, count):
    """Apply updates to the pending tuples count"""
    self.update_reduced_metric(self.PENDING_ACKED_COUNT, count)

  def timeout_tuple(self, stream_id):
    """Apply updates to the timeout count"""
    self.update_count(self.TIMEOUT_COUNT, key=stream_id)

class BoltMetrics(ComponentMetrics):
  """Metrics helper class for Bolt"""
  ACK_COUNT = "__ack-count"
  PROCESS_LATENCY = "__process-latency"
  EXEC_COUNT = "__execute-count"
  EXEC_LATENCY = "__execute-latency"
  EXEC_TIME_NS = "__execute-time-ns"
  TUPLE_DESERIALIZATION_TIME_NS = "__tuple-deserialization-time-ns"

  bolt_metrics = {ACK_COUNT: MultiCountMetric(),
                  PROCESS_LATENCY: MultiMeanReducedMetric(),
                  EXEC_COUNT: MultiCountMetric(),
                  EXEC_LATENCY: MultiMeanReducedMetric(),
                  EXEC_TIME_NS: MultiCountMetric(),
                  TUPLE_DESERIALIZATION_TIME_NS: MultiCountMetric()}

  inputs_init = [ACK_COUNT, ComponentMetrics.FAIL_COUNT,
                 EXEC_COUNT, EXEC_TIME_NS]
  outputs_init = [ComponentMetrics.EMIT_COUNT]

  def __init__(self, pplan_helper):
    super(BoltMetrics, self).__init__(self.bolt_metrics)
    self._init_multi_count_metrics(pplan_helper)

  def _init_multi_count_metrics(self, pplan_helper):
    """Initializes the default values for a necessary set of MultiCountMetrics"""
    # inputs
    to_in_init = [self.metrics[i] for i in self.inputs_init
                  if i in self.metrics and isinstance(self.metrics[i], MultiCountMetric)]
    for in_stream in pplan_helper.get_my_bolt().inputs:
      stream_id = in_stream.stream.id
      global_stream_id = in_stream.stream.component_name + "/" + stream_id
      for metric in to_in_init:
        metric.add_key(stream_id)
        metric.add_key(global_stream_id)
    # outputs
    to_out_init = [self.metrics[i] for i in self.outputs_init
                   if i in self.metrics and isinstance(self.metrics[i], MultiCountMetric)]
    for out_stream in pplan_helper.get_my_bolt().outputs:
      stream_id = out_stream.stream.id
      for metric in to_out_init:
        metric.add_key(stream_id)

  def execute_tuple(self, stream_id, source_component, latency_in_ns):
    """Apply updates to the execute metrics"""
    self.update_count(self.EXEC_COUNT, key=stream_id)
    self.update_reduced_metric(self.EXEC_LATENCY, latency_in_ns, stream_id)
    self.update_count(self.EXEC_TIME_NS, incr_by=latency_in_ns, key=stream_id)

    global_stream_id = source_component + "/" + stream_id
    self.update_count(self.EXEC_COUNT, key=global_stream_id)
    self.update_reduced_metric(self.EXEC_LATENCY, latency_in_ns, global_stream_id)
    self.update_count(self.EXEC_TIME_NS, incr_by=latency_in_ns, key=global_stream_id)

  def deserialize_data_tuple(self, stream_id, source_component, latency_in_ns):
    """Apply updates to the deserialization metrics"""
    self.update_count(self.TUPLE_DESERIALIZATION_TIME_NS, incr_by=latency_in_ns, key=stream_id)
    global_stream_id = source_component + "/" + stream_id
    self.update_count(self.TUPLE_DESERIALIZATION_TIME_NS, incr_by=latency_in_ns,
                      key=global_stream_id)

  def acked_tuple(self, stream_id, source_component, latency_in_ns):
    """Apply updates to the ack metrics"""
    self.update_count(self.ACK_COUNT, key=stream_id)
    self.update_reduced_metric(self.PROCESS_LATENCY, latency_in_ns, stream_id)
    global_stream_id = source_component + '/' + stream_id
    self.update_count(self.ACK_COUNT, key=global_stream_id)
    self.update_reduced_metric(self.PROCESS_LATENCY, latency_in_ns, global_stream_id)

  def failed_tuple(self, stream_id, source_component, latency_in_ns):
    """Apply updates to the fail metrics"""
    self.update_count(self.FAIL_COUNT, key=stream_id)
    self.update_reduced_metric(self.FAIL_LATENCY, latency_in_ns, stream_id)
    global_stream_id = source_component + '/' + stream_id
    self.update_count(self.FAIL_COUNT, key=global_stream_id)
    self.update_reduced_metric(self.FAIL_LATENCY, latency_in_ns, global_stream_id)

class MetricsCollector(object):
  """Helper class for pushing metrics to Out-Metrics queue"""
  def __init__(self, looper, out_metrics):
    self.looper = looper
    # map <metrics name -> IMetric object>
    self.metrics_map = dict()
    # map <time_bucket_sec -> metrics name>
    self.time_bucket_in_sec_to_metrics_name = dict()
    # out metrics queue
    self.out_metrics = out_metrics

  def register_metric(self, name, metric, time_bucket_in_sec):
    """Registers a given metric

    :param name: name of the metric
    :param metric: IMetric object to be registered
    :param time_bucket_in_sec: time interval for update to the metrics manager
    """
    if name in self.metrics_map:
      raise RuntimeError("Another metric has already been registered with name: %s" % name)

    Log.debug("Register metric: %s, with interval: %s", name, str(time_bucket_in_sec))
    self.metrics_map[name] = metric

    if time_bucket_in_sec in self.time_bucket_in_sec_to_metrics_name:
      self.time_bucket_in_sec_to_metrics_name[time_bucket_in_sec].append(name)
    else:
      self.time_bucket_in_sec_to_metrics_name[time_bucket_in_sec] = [name]
      self._register_timer_task(time_bucket_in_sec)

  def _gather_metrics(self, time_bucket_in_sec):
    if time_bucket_in_sec in self.time_bucket_in_sec_to_metrics_name:
      message = metrics_pb2.MetricPublisherPublishMessage()
      for name in self.time_bucket_in_sec_to_metrics_name[time_bucket_in_sec]:
        Log.debug("Will call gather_one_metric with %s", name)
        self._gather_one_metric(name, message)

      assert message.IsInitialized()
      self.out_metrics.offer(message)

      # schedule ourselves again
      self._register_timer_task(time_bucket_in_sec)

  def _register_timer_task(self, time_bucket_in_sec):
    task = lambda: self._gather_metrics(time_bucket_in_sec)
    self.looper.register_timer_task_in_sec(task, time_bucket_in_sec)

  def _gather_one_metric(self, name, message):
    metric_value = self.metrics_map[name].get_value_and_reset()
    Log.debug("In gather_one_metric with name: %s, and value: %s", name, str(metric_value))

    if metric_value is None:
      return
    elif isinstance(metric_value, dict):
      for key, value in list(metric_value.items()):
        if key is not None and value is not None:
          self._add_data_to_message(message, name + "/" + str(key), value)
          self._add_data_to_message(message, "%s/%s" % (name, str(key)), value)
        else:
          Log.info("When gathering metric: %s, <%s:%s> is not a valid key-value to output "
                   "as metric. Skipping...", name, str(key), str(value))
          continue
    else:
      self._add_data_to_message(message, name, metric_value)

  @staticmethod
  def _add_data_to_message(message, metric_name, metric_value):
    if isinstance(metric_value, metrics_pb2.MetricDatum):
      to_add = message.metrics.add()
      to_add.CopyFrom(metric_value)
    elif isinstance(metric_value, metrics_pb2.ExceptionData):
      to_add = message.exceptions.add()
      to_add.CopyFrom(metric_value)
    else:
      assert metric_value is not None
      datum = metrics_pb2.MetricDatum()
      datum.name = metric_name
      datum.value = str(metric_value)
      to_add = message.metrics.add()
      to_add.CopyFrom(datum)
