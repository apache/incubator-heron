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

import unittest

from heron.common.src.python.utils.misc import PhysicalPlanHelper
from heron.proto import topology_pb2

import heron.instance.tests.python.mock_generator as mock_generator

class PhysicalPlanHelperTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_sample_success(self):

    # Instance 1 should be Spout 1
    pplan, instances = mock_generator.get_a_sample_pplan(with_detail=True)
    instance_1 = instances[0]

    pplan_helper = PhysicalPlanHelper(pplan, instance_1["instance_id"])
    self.assertIsNotNone(pplan_helper.my_instance)
    self.assertTrue(pplan_helper.is_spout)
    self.assertIsInstance(pplan_helper.get_my_spout(), topology_pb2.Spout)
    self.assertIsNone(pplan_helper.get_my_bolt())

    self.assertEqual(instance_1["task_id"], pplan_helper.my_task_id)
    self.assertEqual(instance_1["comp_name"], pplan_helper.my_component_name)
