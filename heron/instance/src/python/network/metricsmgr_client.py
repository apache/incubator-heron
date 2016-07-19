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

from heron.common.src.python.log import Log
from heron.proto import metrics_pb2, common_pb2
from heron.instance.src.python.network.heron_client import HeronClient
from heron.instance.src.python.network.protocol import StatusCode

class MetricsManagerClient(HeronClient):
  def __init__(self, metrics_host, port, instance, metrics_out_queue, gateway_metrics, sock_map):
    HeronClient.__init__(self, metrics_host, port, sock_map)
    self.instance = instance
    self.out_queue = metrics_out_queue
    self.gateway_metrics = gateway_metrics

  def on_connect(self, status):
    Log.debug("In on_connect of MetricsManagerClient")
    self._send_register_req()

  def on_response(self, status, context, response):
    Log.debug("In on_response with status: " + str(status))
    if status != StatusCode.OK:
      raise RuntimeError("Response from Metrics Manager not OK")
    if isinstance(response, metrics_pb2.MetricPublisherRegisterResponse):
      self._handle_register_response(response)
    else:
      Log.error("Weird kind: " + response.DESCRIPTOR.full_name)
      raise RuntimeError("Unknown kind of response received from Metrics Manager")

  def on_incoming_message(self, message):
    raise RuntimeError("Metrics Client got an unknown message from Metrics Manager")

  def _send_register_req(self):
    hostname = socket.gethostname()

    metric_publisher = metrics_pb2.MetricPublisher()
    metric_publisher.hostname = hostname
    metric_publisher.port = self.instance.info.task_id
    metric_publisher.component_name = self.instance.info.component_name
    metric_publisher.instance_id = self.instance.instance_id
    metric_publisher.instance_index = self.instance.info.component_index

    request = metrics_pb2.MetricsPublisherRegisterRequest()
    request.publisher.CopyFrom(metric_publisher)

    self.send_request(request, "MetricsClientContext",
                      metrics_pb2.MetricPublisherRegisterResponse(), 10)

  def _handle_register_response(self, response):
    if response.status.status != common_pb2.StatusCode.Value("OK"):
      raise RuntimeError("Stream Manager returned a not OK response for register")
    Log.info("We registered ourselves to the Metrics Manager")

