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

class MetricsTest(unittest.TestCase):
  def test_count_metric(self):
    metric = CountMetric()
    for _ in range(10):
      metric.incr()
    self.assertEqual(metric.get_value_and_reset(), 10)

    for _ in range(10):
      metric.incr(to_add=10)
    self.assertEqual(metric.get_value_and_reset(), 100)
    self.assertEqual(metric.get_value_and_reset(), 0)

  def test_multi_count_metric(self):
    metric = MultiCountMetric()
    key_list = ["key1", "key2", "key3"]
    for _ in range(10):
      for key in key_list:
        metric.incr(key=key)
    self.assertEqual(metric.get_value_and_reset(), dict(list(zip(key_list, [10] * 3))))
    self.assertEqual(metric.get_value_and_reset(), dict(list(zip(key_list, [0] * 3))))

    metric.add_key("key4")
    ret = metric.get_value_and_reset()
    self.assertIn("key4", ret)
    self.assertEqual(ret["key4"], 0)

  def test_mean_reduced_metric(self):
    metric = MeanReducedMetric()
    # update from 1 to 10
    for i in range(1, 11):
      metric.update(i)
    self.assertEqual(metric.get_value_and_reset(), 5.5)
    self.assertIsNone(metric.get_value_and_reset())

    for i in range(1, 11):
      metric.update(i * 10)
    self.assertEqual(metric.get_value_and_reset(), 55)

  def test_multi_mean_reduced_metric(self):
    metric = MultiMeanReducedMetric()
    key_list = ["key1", "key2", "key3"]
    for i in range(1, 11):
      metric.update(key=key_list[0], value=i)
      metric.update(key=key_list[1], value=i * 2)
      metric.update(key=key_list[2], value=i * 3)
    self.assertEqual(metric.get_value_and_reset(), dict(list(zip(key_list, [5.5, 11, 16.5]))))
    self.assertEqual(metric.get_value_and_reset(), dict(list(zip(key_list, [None] * 3))))

    metric.add_key("key4")
    ret = metric.get_value_and_reset()
    self.assertIn("key4", ret)
    self.assertIsNone(ret["key4"])
