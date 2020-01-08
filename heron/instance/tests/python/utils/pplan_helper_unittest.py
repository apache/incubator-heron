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

from heron.instance.src.python.utils.misc import PhysicalPlanHelper
from heron.proto import topology_pb2

import heron.instance.tests.python.utils.mock_generator as mock_generator
import heron.instance.tests.python.mock_protobuf as mock_protobuf

class PhysicalPlanHelperTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_sample_success(self):

    # Instance 1 should be Spout 1
    pplan, instances = mock_generator.get_a_sample_pplan()
    instance_1 = instances[0]

    pplan_helper = PhysicalPlanHelper(pplan, instance_1["instance_id"], "topology.pex.path")
    self.assertIsNotNone(pplan_helper.my_instance)
    self.assertTrue(pplan_helper.is_spout)
    self.assertIsInstance(pplan_helper.get_my_spout(), topology_pb2.Spout)
    self.assertIsNone(pplan_helper.get_my_bolt())

    self.assertEqual(instance_1["task_id"], pplan_helper.my_task_id)
    self.assertEqual(instance_1["comp_name"], pplan_helper.my_component_name)

  # pylint: disable=protected-access
  def test_number_autotype(self):
    # testing _is_number() and _get_number()

    number_strings = ["1", "20", "30", "123410000000000", "12.40", "3.1", "3.000"]
    for test_str in number_strings:
      ret = PhysicalPlanHelper._is_number(test_str)
      self.assertTrue(ret)

      ret = PhysicalPlanHelper._get_number(test_str)
      self.assertIsInstance(ret, (int, float))

    non_number_strings = ["1a", "1,000", "string", "1.51hello"]
    for test_str in non_number_strings:
      ret = PhysicalPlanHelper._is_number(test_str)
      self.assertFalse(ret)

  def test_get_dict_from_config(self):
    # testing creating auto-typed dict from protobuf Config message

    # try with config <str -> str>
    test_config_1 = {"key1": "string_value",
                     "key2": "12",
                     "key3": "1.50",
                     "key4": "false"}
    expected_1 = {"key1": "string_value",
                  "key2": 12,
                  "key3": 1.50,
                  "key4": False}
    proto_config = mock_protobuf.get_mock_config(config_dict=test_config_1)
    ret = PhysicalPlanHelper._get_dict_from_config(proto_config)
    self.assertEqual(ret, expected_1)

    # try with config <str -> any object>
    test_config_2 = {"key1": "string",
                     "key2": True,
                     "key3": 1.20,
                     "key4": (1, 2, 3, "hello"),
                     "key5": {"key5_1": 1, "key5_2": "value"},
                     "key6": [1, 2, 3, 4, 5.0]}

    proto_config = mock_protobuf.get_mock_config(config_dict=test_config_2)
    ret = PhysicalPlanHelper._get_dict_from_config(proto_config)
    self.assertEqual(ret, test_config_2)
