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

import unittest
import threading

from heronpy.api import global_metrics
import heron.instance.tests.python.utils.mock_generator as mock_generator

class GlobalMetricsTest(unittest.TestCase):
  def setUp(self):
    self.metrics_collector = mock_generator.MockMetricsCollector()
    global_metrics.init(self.metrics_collector, 10)
    self.lock = threading.Lock()

  def test_normal(self):
    global_metrics.incr("mycounter_a")
    global_metrics.incr("mycounter_b", 3)
    global_metrics.safe_incr("mycounter_c", 5)
    counter = global_metrics.metricsContainer
    d = counter.get_value_and_reset()
    self.assertTrue("mycounter_a" in d)
    self.assertTrue("mycounter_b" in d)
    self.assertTrue("mycounter_c" in d)
    self.assertEqual(d["mycounter_a"], 1)
    self.assertEqual(d["mycounter_b"], 3)
    self.assertEqual(d["mycounter_c"], 5)

  def concurrent_incr(self):
    def incr_worker():
      global_metrics.safe_incr("K")
      global_metrics.safe_incr("K", 2)
      global_metrics.safe_incr("K", 3)
    threads = []
    for i in range(10):
      t = threading.Thread(target=incr_worker)
      threads.append(t)
      t.start()
    for t in threads:
      t.join()
    counter = global_metrics.metricsContainer
    d = counter.get_value_and_reset()
    self.assertTrue("K" in d)
    self.assertEqual(d["K"], 60)

  def test_concurrent_incr(self):
    for i in range(100):
      global_metrics.metricsContainer.get_value_and_reset()
      self.concurrent_incr()
