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

from heron.instance.src.python.utils.topology import TopologyContextImpl

import heron.instance.tests.python.utils.mock_generator as mock_generator
import heron.instance.tests.python.mock_protobuf as mock_protobuf

class TopologyContextImplTest(unittest.TestCase):
  def setUp(self):
    self.context = TopologyContextImpl(
      config={},
      topology=mock_protobuf.get_mock_topology(),
      task_to_component={},
      my_task_id="task_id",
      metrics_collector=None,
      topo_pex_path="path.to.pex")

  def test_task_hook(self):
    task_hook = mock_generator.MockTaskHook()
    self.assertFalse(len(self.context.task_hooks) > 0)
    self.context.add_task_hook(task_hook)
    self.assertTrue(len(self.context.task_hooks) > 0)

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
