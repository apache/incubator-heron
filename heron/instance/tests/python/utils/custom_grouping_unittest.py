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
from heron.instance.src.python.utils.misc import CustomGroupingHelper
from heron.instance.tests.python.utils.mock_generator import MockCustomGrouping

class CustomGroupingTest(unittest.TestCase):
  STREAM_ID = "default"
  TASK_IDS = [2, 3, 5, 7, 11, 13, 17]
  SRC_COMP_NAME = "component1"
  VALUES = ['test', 'values']

  def setUp(self):
    self.grouper = CustomGroupingHelper()

  def tearDown(self):
    self.grouper = None

  def test_all_target(self):
    # custom grouping returns all targets
    all_grouping = MockCustomGrouping(MockCustomGrouping.ALL_TARGET_MODE)
    self.grouper.add(self.STREAM_ID, self.TASK_IDS, all_grouping, self.SRC_COMP_NAME)
    self.assertIn(self.STREAM_ID, self.grouper.targets)

    self.grouper.prepare(None)
    ret = self.grouper.choose_tasks(self.STREAM_ID, self.VALUES)
    self.assertEqual(ret, self.TASK_IDS)
    ret2 = self.grouper.choose_tasks("not_registered_stream", self.VALUES)
    self.assertIsNotNone(ret2)
    self.assertIsInstance(ret2, list)
    self.assertEqual(len(ret2), 0)

  def test_random_target(self):
    # custom grouping returns a random subset of given targets
    random_grouping = MockCustomGrouping(MockCustomGrouping.RANDOM_TARGET_MODE)
    self.grouper.add(self.STREAM_ID, self.TASK_IDS, random_grouping, self.SRC_COMP_NAME)
    self.grouper.prepare(None)
    ret = self.grouper.choose_tasks(self.STREAM_ID, self.VALUES)
    self.assertEqual(len(set(ret)), len(ret))
    self.assertTrue(set(ret) <= set(self.TASK_IDS))

  def test_wrong_return_type(self):
    wrong_rettype = MockCustomGrouping(MockCustomGrouping.WRONG_RETURN_TYPE_MODE)
    self.grouper.add(self.STREAM_ID, self.TASK_IDS, wrong_rettype, self.SRC_COMP_NAME)
    self.grouper.prepare(None)

    with self.assertRaises(TypeError):
      self.grouper.choose_tasks(self.STREAM_ID, self.VALUES)

  def test_wrong_return_value(self):
    wrong_retvalue = MockCustomGrouping(MockCustomGrouping.WRONG_CHOOSE_TASK_MODE)
    self.grouper.add(self.STREAM_ID, self.TASK_IDS, wrong_retvalue, self.SRC_COMP_NAME)
    self.grouper.prepare(None)

    with self.assertRaises(ValueError):
      self.grouper.choose_tasks(self.STREAM_ID, self.VALUES)
