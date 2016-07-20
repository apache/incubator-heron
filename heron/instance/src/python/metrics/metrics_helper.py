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
from heron.common.src.python.log import Log
from heron.proto import metrics_pb2
from heron.common.src.python.utils.metrics import CountMetric, MultiCountMetric, MeanReducedMetric, ReducedMetric, MultiMeanReducedMetric, MultiReducedMetric
import heron.common.src.python.constants as constants


class GatewayMetrics(object):
  RECEIVED_PKT_SIZE = '__gateway-received-packets-size'
  SENT_PKT_SIZE = '__gateway-sent-packets-size'
  RECEIVED_PKT_COUNT = '__gateway-received-packets-count'
  SENT_PKT_COUNT = '__gateway-sent-packets-count'

  def __init__(self, metric_collector, sys_config):
    self.metric_collector = metric_collector
    self.metrics = {
      self.RECEIVED_PKT_SIZE: CountMetric(),
      self.SENT_PKT_SIZE: CountMetric(),
      self.RECEIVED_PKT_COUNT: CountMetric(),
      self.SENT_PKT_COUNT: CountMetric()
    }
    interval = float(sys_config[constants.METRICS_EXPORT_INTERVAL_SECS])

    for key, value in self.metrics.iteritems():
      metric_collector.register_metric(key, value, interval)

  def update_count(self, name, incr_by=1):
    if name not in self.metrics:
      Log.error("In update_count: " + name + " is not registered in the metric")
      return
    self.metrics[name].incr(incr_by)

# TODO: seriously reconsider the design of this again
class ComponentMetrics(object):
  """Metrics to be collected for both Bolt and Spout"""
  ACK_COUNT = "__ack-count"
  FAIL_LATENCY = "__fail-latency"
  FAIL_COUNT = "__fail-count"
  EMIT_COUNT = "__emit-count"
  TUPLE_SERIALIZATION_TIME_NS = "__tuple-serialization-time-ns"
  OUT_QUEUE_FULL_COUNT = "__out-queue-full-count"

  component_metrics = {
    ACK_COUNT: MultiCountMetric(),
    FAIL_LATENCY: MultiMeanReducedMetric(),
    FAIL_COUNT: MultiCountMetric(),
    EMIT_COUNT: MultiCountMetric(),
    TUPLE_SERIALIZATION_TIME_NS: MultiCountMetric(),
    OUT_QUEUE_FULL_COUNT: CountMetric()
  }

  def __init__(self, additional_metrics):
    self.metrics = self.component_metrics
    self.metrics.update(additional_metrics)

  def register_metrics(self, context, sys_config):
    interval = float(sys_config[constants.METRICS_EXPORT_INTERVAL_SECS])
    for key, value in self.metrics.iteritems():
      context.register_metric(key, value, interval)

  def update_count(self, name, key=None, incr_by=1):
    """Update the value of CountMetric or MultiCountMetric"""
    if name not in self.metrics:
      Log.error("In update_count(): " + name + " is not registered in the metric")

    if key is None and isinstance(self.metrics[name], CountMetric):
      self.metrics[name].incr(incr_by)
    elif key is not None and isinstance(self.metrics[name], MultiCountMetric):
      self.metrics[name].incr(key, incr_by)
    else:
      Log.error("In update_count(): " + name + " is registered but not supported with this method ")

  def update_reduced_metric(self, name, value, key=None):
    """Update the value of ReducedMetric or MultiReducedMetric"""
    if name not in self.metrics:
      Log.error("In update_reduced_metric(): " + name + " is not registered in the metric")

    if key is None and isinstance(self.metrics[name], ReducedMetric):
      self.metrics[name].update(value)
    elif key is not None and isinstance(self.metrics[name], MultiReducedMetric):
      self.metrics[name].update(key, value)
    else:
      Log.error("In update_count(): " + name + " is registered but not supported with this method")

  def update_out_queue_full_count(self):
    self.update_count(self.OUT_QUEUE_FULL_COUNT)

  def update_emit_count(self, stream_id):
    self.update_count(self.EMIT_COUNT, key=stream_id)


class SpoutMetrics(ComponentMetrics):
  COMPLETE_LATENCY = "__complete-latency"
  TIMEOUT_COUNT = "__timeout-count"
  NEXT_TUPLE_LATENCY = "__next-tuple-latency"
  NEXT_TUPLE_COUNT = "__next-tuple-count"
  PENDING_ACKED_COUNT = "__pending-acked-count"

  spout_metrics = {
    COMPLETE_LATENCY: MultiMeanReducedMetric(),
    TIMEOUT_COUNT: MultiCountMetric(),
    NEXT_TUPLE_LATENCY: MeanReducedMetric(),
    NEXT_TUPLE_COUNT: CountMetric(),
    PENDING_ACKED_COUNT: MeanReducedMetric()
  }

  to_multi_init = [ComponentMetrics.ACK_COUNT, ComponentMetrics.FAIL_COUNT,
                   TIMEOUT_COUNT, ComponentMetrics.EMIT_COUNT]

  def __init__(self, pplan_helper):
    super(SpoutMetrics, self).__init__(self.spout_metrics)
    self._init_multi_count_metrics(pplan_helper)

  def _init_multi_count_metrics(self, pplan_helper):
    to_init = [self.metrics[i] for i in self.to_multi_init
               if i in self.metrics and isinstance(self.metrics[i], MultiCountMetric)]
    for out_stream in pplan_helper.get_my_spout().outputs:
      stream_id = out_stream.stream.id
      for metric in to_init:
        metric.add_key(stream_id)

  def next_tuple(self, latency_in_ns):
    self.update_reduced_metric(self.NEXT_TUPLE_LATENCY, latency_in_ns)
    self.update_count(self.NEXT_TUPLE_COUNT)

  def acked_tuple(self, stream_id, complete_latency_ns):
    self.update_count(self.ACK_COUNT, stream_id)
    self.update_reduced_metric(self.COMPLETE_LATENCY, complete_latency_ns, stream_id)

  def failed_tuple(self, stream_id, fail_latency_ns):
    self.update_count(self.FAIL_COUNT, stream_id)
    self.update_reduced_metric(self.FAIL_LATENCY, fail_latency_ns, stream_id)

  def update_pending_tuples_count(self, count):
    self.update_reduced_metric(self.PENDING_ACKED_COUNT, count)


class BoltMetrics(ComponentMetrics):
  PROCESS_LATENCY = "__process-latency"
  EXEC_COUNT = "__execute-count"
  EXEC_LATENCY = "__execute-latency"
  EXEC_TIME_NS = "__execute-time-ns"
  TUPLE_DESERIALIZATION_TIME_NS = "__tuple-deserialization-time-ns"

  bolt_metrics = {
    EXEC_COUNT: MultiCountMetric(),
    EXEC_LATENCY: MultiMeanReducedMetric(),
    EXEC_TIME_NS: MultiCountMetric()
  }

  inputs_init = [ComponentMetrics.ACK_COUNT, ComponentMetrics.FAIL_COUNT,
                 EXEC_COUNT, EXEC_TIME_NS]
  outputs_init = [ComponentMetrics.EMIT_COUNT]

  def __init__(self, pplan_helper):
    super(BoltMetrics, self).__init__(self.bolt_metrics)
    self._init_multi_count_metrics(pplan_helper)

  def _init_multi_count_metrics(self, pplan_helper):
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
    self.update_count(self.EXEC_COUNT, stream_id)
    self.update_reduced_metric(self.EXEC_LATENCY, latency_in_ns, stream_id)
    self.update_count(self.EXEC_TIME_NS, stream_id, incr_by=latency_in_ns)

    global_stream_id = source_component + "/" + stream_id
    self.update_count(self.EXEC_COUNT, global_stream_id)
    self.update_reduced_metric(self.EXEC_LATENCY, latency_in_ns, global_stream_id)
    self.update_count(self.EXEC_TIME_NS, global_stream_id, incr_by=latency_in_ns)

class MetricsCollector(object):
  """Helper class for pushing metrics to Out-Metrics queue"""
  def __init__(self, looper, out_metrics):
    self.looper = looper
    self.metrics_map = dict()
    self.time_bucket_in_sec_to_metrics_name = dict()
    self.out_metrics = out_metrics

  def register_metric(self, name, metric, time_bucket_in_sec):
    if name in self.metrics_map:
      raise RuntimeError("Another metric has already been registered with name: " + name)

    Log.debug("Register metric: " + name + ", with interval: " + str(time_bucket_in_sec))
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
        Log.debug("Will call gather_one_metric with " + name)
        self._gather_one_metric(name, message)

      assert message.IsInitialized()
      self.out_metrics.offer(message)

      # schedule ourselves again
      self._register_timer_task(time_bucket_in_sec)

  def _register_timer_task(self, time_bucket_in_sec):
    task = lambda : self._gather_metrics(time_bucket_in_sec)
    self.looper.register_timer_task_in_sec(task, time_bucket_in_sec)

  def _gather_one_metric(self, name, message):
    metric_value = self.metrics_map[name].get_value_and_reset()
    Log.debug("In gather_one_metric with name: " + name + ", and value: " + str(metric_value))

    if metric_value is None:
      return
    elif isinstance(metric_value, dict):
      for key, value in metric_value.iteritems():
        if key is not None and value is not None:
          self._add_data_to_message(message, name + "/" + str(key), value)
        else:
          Log.error("<" + str(key) + ":" + str(value) + "> is not a valid key-value to output as metric")
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



