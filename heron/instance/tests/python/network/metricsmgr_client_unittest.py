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


# pylint: disable=missing-docstring
# pylint: disable=protected-access
import unittest

from heron.instance.src.python.network import StatusCode
from heron.instance.tests.python.network.mock_generator import MockMetricsManagerClient

class MetricsMgrClientTest(unittest.TestCase):
  def setUp(self):
    self.metrics_client = MockMetricsManagerClient()

  def tearDown(self):
    self.metrics_client = None

  def test_add_task(self):
    # _add_metrics_client_tasks() should have been already called
    self.assertEqual(len(self.metrics_client.looper.wakeup_tasks), 1)
    self.assertEqual(self.metrics_client.looper.wakeup_tasks[0].__name__, "_send_metrics_messages")

  def test_on_connect_ok(self):
    self.metrics_client.on_connect(StatusCode.OK)
    self.assertTrue(self.metrics_client.register_req_called)

  def test_on_connect_error(self):
    self.metrics_client.on_connect(StatusCode.CONNECT_ERROR)
    self.assertEqual(len(self.metrics_client.looper.timer_tasks), 3)
    self.assertEqual(self.metrics_client.looper.timer_tasks[0][1].__name__, '_update_in_out_stream_metrics_tasks')
    self.assertEqual(self.metrics_client.looper.timer_tasks[1][1].__name__, '_update_py_metrics')
    self.assertEqual(self.metrics_client.looper.timer_tasks[2][1].__name__, 'start_connect')

  def test_on_error(self):
    self.metrics_client.on_error()
    self.assertEqual(len(self.metrics_client.looper.timer_tasks), 3)
    self.assertEqual(self.metrics_client.looper.timer_tasks[0][1].__name__, '_update_in_out_stream_metrics_tasks')
    self.assertEqual(self.metrics_client.looper.timer_tasks[1][1].__name__, '_update_py_metrics')
    self.assertEqual(self.metrics_client.looper.timer_tasks[2][1].__name__, 'start_connect')
