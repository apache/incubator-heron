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
import collections
from abc import abstractmethod
from heron.common.src.python.log import Log
from heron.proto import metrics_pb2

class IMetric(object):
  @abstractmethod
  def get_value_and_reset(self):
    pass

class CountMetric(IMetric):
  def __init__(self):
    self.value = 0

  def incr(self, to_add=1):
    self.value += to_add

  def get_value_and_reset(self):
    ret = self.value
    self.value = 0
    return ret

class GatewayMetrics(object):
  RECEIVED_PKT_SIZE = '__gateway-received-packets-size'
  SENT_PKT_SIZE = '__gateway-sent-packets-size'
  RECEIVED_PKT_COUNT = '__gateway-received-packets-count'
  SENT_PKT_COUNT = '__gateway-sent-packets-count'

  def __init__(self, metrics_helper):
    self.metrics_helper = metrics_helper
    self.metrics = {
      self.RECEIVED_PKT_SIZE: CountMetric(),
      self.SENT_PKT_SIZE: CountMetric(),
      self.RECEIVED_PKT_COUNT: CountMetric(),
      self.SENT_PKT_COUNT: CountMetric()
    }
    # TODO: change hardcoding
    interval = 3.0

    for key, value in self.metrics.iteritems():
      metrics_helper.register_metric(key, value, interval)

  def update_count(self, name, incr_by=1):
    if name not in self.metrics:
      Log.error("In update_count: " + name + " is not registered in the metric")
      return
    self.metrics[name].incr(incr_by)

class MetricsHelper(object):
  """Helper class for pushing metrics to Out-Metrics queue"""
  def __init__(self, looper, out_metrics):
    self.looper = looper
    self.metrics_map = dict()
    self.time_bucket_in_sec_to_metrics_name = dict()
    self.out_metrics = out_metrics

  def register_metric(self, name, metric, time_bucket_in_sec):
    if name in self.metrics_map:
      raise RuntimeError("Another metric has already been registered with name: " + name)

    Log.debug("Register metric: " + name)
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

    # TODO: currently only treat value as str
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



