#!/usr/bin/env python3
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

'''metrics manager client'''
import socket

from heron.common.src.python.utils.log import Log

from heron.instance.src.python.network.heron_client import HeronClient
from heron.instance.src.python.network import StatusCode
from heron.instance.src.python.utils import system_config

from heron.proto import metrics_pb2, common_pb2

import heron.instance.src.python.utils.system_constants as constants

class MetricsManagerClient(HeronClient):
  """MetricsManagerClient, responsible for communicating with Metrics Manager"""
  # pylint: disable=too-many-arguments
  def __init__(self, looper, metrics_host, port, instance,
               out_metrics, in_stream, out_stream, sock_map, socket_options,
               gateway_metrics, py_metrics):
    HeronClient.__init__(self, looper, metrics_host, port, sock_map, socket_options)
    self.instance = instance
    self.out_queue = out_metrics
    self.in_stream = in_stream
    self.out_stream = out_stream
    self.gateway_metrics = gateway_metrics
    self.py_metrics = py_metrics
    self.sys_config = system_config.get_sys_config()

    self._add_metrics_client_tasks()
    Log.debug('start updating in and out stream metrics')
    self._update_in_out_stream_metrics_tasks()
    self._update_py_metrics()

  def _add_metrics_client_tasks(self):
    self.looper.add_wakeup_task(self._send_metrics_messages)

  def _update_in_out_stream_metrics_tasks(self):
    in_size, out_size = self.in_stream.get_size(), self.out_stream.get_size()
    Log.debug("updating in and out stream metrics, %d, %d", in_size, out_size)
    self.gateway_metrics.update_in_out_stream_metrics(in_size, out_size, in_size, out_size)
    interval = float(self.sys_config[constants.INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC])
    self.looper.register_timer_task_in_sec(self._update_in_out_stream_metrics_tasks, interval)

  def _update_py_metrics(self):
    self.py_metrics.update_all()
    interval = float(self.sys_config[constants.INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC])
    self.looper.register_timer_task_in_sec(self._update_py_metrics, interval)

  def _send_metrics_messages(self):
    if self.connected:
      while not self.out_queue.is_empty():
        message = self.out_queue.poll()
        assert isinstance(message, metrics_pb2.MetricPublisherPublishMessage)
        Log.debug(f"Sending metric message: {str(message)}")
        self.send_message(message)
        self.gateway_metrics.update_sent_metrics_size(message.ByteSize())
        self.gateway_metrics.update_sent_metrics(len(message.metrics), len(message.exceptions))

  def on_connect(self, status):
    Log.debug("In on_connect of MetricsManagerClient")
    if status != StatusCode.OK:
      Log.error(f"Error connecting to Metrics Manager with status: {str(status)}")
      retry_interval = float(self.sys_config[constants.INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC])
      self.looper.register_timer_task_in_sec(self.start_connect, retry_interval)
      return
    self._send_register_req()

  def on_response(self, status, context, response):
    Log.debug(f"In on_response with status: {str(status)}, with context: {str(context)}")
    if status != StatusCode.OK:
      raise RuntimeError("Response from Metrics Manager not OK")
    if isinstance(response, metrics_pb2.MetricPublisherRegisterResponse):
      self._handle_register_response(response)
    else:
      Log.error(f"Unknown kind of response received: {response.DESCRIPTOR.full_name}")
      raise RuntimeError("Unknown kind of response received from Metrics Manager")

  # pylint: disable=no-self-use
  def on_incoming_message(self, message):
    raise RuntimeError(f"Metrics Client got an unknown message from "
                       f"Metrics Manager: {str(message)}")

  def on_error(self):
    Log.error("Disconnected from Metrics Manager")
    self.on_connect(StatusCode.CONNECT_ERROR)

  def _send_register_req(self):
    hostname = socket.gethostname()

    metric_publisher = metrics_pb2.MetricPublisher()
    metric_publisher.hostname = hostname
    metric_publisher.port = self.instance.info.task_id
    metric_publisher.component_name = self.instance.info.component_name
    metric_publisher.instance_id = self.instance.instance_id
    metric_publisher.instance_index = self.instance.info.component_index

    request = metrics_pb2.MetricPublisherRegisterRequest()
    request.publisher.CopyFrom(metric_publisher)

    Log.debug(f"Sending MetricsCli register request: \n{str(request)}")

    timeout_sec = float(self.sys_config[constants.INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC])
    self.send_request(request, "MetricsClientContext",
                      metrics_pb2.MetricPublisherRegisterResponse(), timeout_sec)

  # pylint: disable=no-self-use
  def _handle_register_response(self, response):
    if response.status.status != common_pb2.StatusCode.Value("OK"):
      raise RuntimeError("Metrics Manager returned a not OK response for register")
    Log.info("We registered ourselves to the Metrics Manager")
