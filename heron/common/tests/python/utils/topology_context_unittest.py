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

# pylint: disable=missing-docstring
import unittest

from heron.common.src.python.utils.topology import TopologyContext

import heron.common.tests.python.utils.mock_generator as mock_generator
import heron.common.tests.python.mock_protobuf as mock_protobuf

class TopologyContextTest(unittest.TestCase):
  def setUp(self):
    self.context = TopologyContext(config={}, topology=mock_protobuf.get_mock_topology(),
                                   task_to_component={}, my_task_id="task_id",
                                   metrics_collector=None, topo_pex_path="path.to.pex")
  def test_task_hook(self):
    task_hook = mock_generator.MockTaskHook()
    self.assertFalse(self.context.hook_exists)
    self.context.add_task_hook(task_hook)
    self.assertTrue(self.context.hook_exists)

    self.context.invoke_hook_prepare()

    self.context.invoke_hook_emit(None, None, None)
    self.assertTrue(task_hook.emit_called)

    self.context.invoke_hook_spout_ack(None, 0.1)
    self.assertTrue(task_hook.spout_ack_called)

    self.context.invoke_hook_spout_fail(None, 0.1)
    self.assertTrue(task_hook.spout_fail_called)

    self.context.invoke_hook_bolt_execute(None, 0.1)
    self.assertTrue(task_hook.bolt_exec_called)

    self.context.invoke_hook_bolt_ack(None, 0.1)
    self.assertTrue(task_hook.bolt_ack_called)

    self.context.invoke_hook_bolt_fail(None, 0.1)
    self.assertTrue(task_hook.bolt_fail_called)
