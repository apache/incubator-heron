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


# pylint: disable=missing-docstring

import unittest

from heronpy.api.metrics import (CountMetric, MultiCountMetric,
                                          MeanReducedMetric, MultiMeanReducedMetric)
from heron.instance.src.python.utils.metrics import BaseMetricsHelper
from heron.proto import metrics_pb2
import heron.instance.tests.python.utils.mock_generator as mock_generator

class BaseMetricsHelperTest(unittest.TestCase):
  def setUp(self):
    self.metrics = {"metric1": CountMetric(),
                    "metric2": MultiCountMetric(),
                    "metric3": MeanReducedMetric(),
                    "metric4": MultiMeanReducedMetric()}
    self.metrics_helper = BaseMetricsHelper(self.metrics)
    self.metrics_collector = mock_generator.MockMetricsCollector()

  def tearDown(self):
    self.metrics = None
    self.metrics_helper = None
    self.metrics_collector = None

  def test_register_metrics(self):
    self.metrics_helper.register_metrics(self.metrics_collector, 60)
    for name, metric in list(self.metrics.items()):
      self.assertEqual(self.metrics_collector.metrics_map[name], metric)
    self.assertEqual(len(self.metrics_collector.time_bucket_in_sec_to_metrics_name[60]), 4)
    self.assertIn(60, self.metrics_collector.registered_timers)

  def test_update_count(self):
    self.metrics_helper.update_count("metric1")
    self.assertEqual(self.metrics["metric1"].get_value_and_reset(), 1)
    self.assertEqual(self.metrics["metric1"].get_value_and_reset(), 0)
    self.metrics_helper.update_count("metric1", incr_by=10)
    self.assertEqual(self.metrics["metric1"].get_value_and_reset(), 10)

    self.metrics_helper.update_count("metric2", key="key1")
    self.assertEqual(self.metrics["metric2"].get_value_and_reset(), {"key1": 1})
    self.assertEqual(self.metrics["metric2"].get_value_and_reset(), {"key1": 0})
    self.metrics_helper.update_count("metric2", incr_by=10, key="key2")
    self.assertEqual(self.metrics["metric2"].get_value_and_reset(), {"key1": 0,
                                                                     "key2": 10})

  def test_update_reduced_metric(self):
    for i in range(1, 11):
      self.metrics_helper.update_reduced_metric("metric3", i)
    self.assertEqual(self.metrics["metric3"].get_value_and_reset(), 5.5)
    self.assertIsNone(self.metrics["metric3"].get_value_and_reset())

    for i in range(1, 11):
      self.metrics_helper.update_reduced_metric("metric4", i, key="key1")
      self.metrics_helper.update_reduced_metric("metric4", i * 2, key="key2")
      self.metrics_helper.update_reduced_metric("metric4", i * 3, key="key3")
    self.assertEqual(self.metrics["metric4"].get_value_and_reset(), {"key1": 5.5,
                                                                     "key2": 11,
                                                                     "key3": 16.5})
    self.assertEqual(self.metrics["metric4"].get_value_and_reset(), {"key1": None,
                                                                     "key2": None,
                                                                     "key3": None})

class MetricsCollectorTest(unittest.TestCase):
  def setUp(self):
    self.metrics_collector = mock_generator.MockMetricsCollector()

  def tearDown(self):
    self.metrics_collector = None

  def test_register_metric(self):
    name1 = "metric1"
    metric1 = CountMetric()
    self.metrics_collector.register_metric(name1, metric1, 60)
    self.assertEqual(self.metrics_collector.metrics_map[name1], metric1)
    self.assertIn(60, self.metrics_collector.registered_timers)

    name2 = "metric2"
    metric2 = MeanReducedMetric()
    self.metrics_collector.register_metric(name2, metric2, 60)
    self.assertEqual(self.metrics_collector.metrics_map[name2], metric2)
    self.assertEqual(self.metrics_collector.time_bucket_in_sec_to_metrics_name[60],
                     [name1, name2])

    name3 = "metric3"
    metric3 = MultiMeanReducedMetric()
    self.metrics_collector.register_metric(name3, metric3, 30)
    self.assertEqual(self.metrics_collector.metrics_map[name3], metric3)
    self.assertEqual(self.metrics_collector.registered_timers, [60, 30])

  # pylint: disable=protected-access
  def test_gather_metrics(self):
    name = "metric"
    metric = CountMetric()
    metric.incr(to_add=10)
    self.metrics_collector.register_metric(name, metric, 60)
    self.assertIn(60, self.metrics_collector.time_bucket_in_sec_to_metrics_name)
    self.metrics_collector._gather_metrics(60)

    message = self.metrics_collector.out_metrics.poll()
    self.assertIsNotNone(message)
    self.assertIsInstance(message, metrics_pb2.MetricPublisherPublishMessage)
    self.assertEqual(message.metrics[0].name, name)
    self.assertEqual(message.metrics[0].value, str(10))
    self.assertEqual(metric.get_value_and_reset(), 0)
