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
from collections import namedtuple
from unittest.mock import Mock, patch
import unittest
import threading

from heron.instance.src.python.utils.metrics.py_metrics import PyMetrics
import heron.instance.src.python.utils.system_constants as constants
import heron.instance.tests.python.utils.mock_generator as mock_generator

class PyMetricsTest(unittest.TestCase):
  def setUp(self):
    metrics_collector = mock_generator.MockMetricsCollector()
    with patch("heron.instance.src.python.utils.system_config.get_sys_config",
               side_effect=lambda: {constants.HERON_METRICS_EXPORT_INTERVAL_SEC: 10}):
          self.py_metrics = PyMetrics(metrics_collector)

  def test_gc(self):
    with patch("gc.get_count", side_effect=lambda:(1, 2, 3)):
      with patch("gc.get_threshold", side_effect=lambda:(4, 5, 6)):
        self.py_metrics.update_gc_stat()
    self.assertEqual(self.py_metrics.g1_count.get_value_and_reset(), 1)
    self.assertEqual(self.py_metrics.g2_count.get_value_and_reset(), 2)
    self.assertEqual(self.py_metrics.g3_count.get_value_and_reset(), 3)
    self.assertEqual(self.py_metrics.g1_threshold.get_value_and_reset(), 4)
    self.assertEqual(self.py_metrics.g2_threshold.get_value_and_reset(), 5)
    self.assertEqual(self.py_metrics.g3_threshold.get_value_and_reset(), 6)

  def test_threads_time(self):
    # self.py_metrics.update_threads_time()
    # tmap = self.py_metrics.threads.get_value_and_reset()
    # self.assertEqual(tmap[100], (3, 5))
    # self.assertEqual(tmap[500], (7, 11))
    pass

  def test_fd_num(self):
    # self.py_metrics.update_fds()
    # self.assertEqual(self.py_metrics.fd_nums.get_value_and_reset(), 3)
    pass
