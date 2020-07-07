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

from heron.instance.src.python.network import StatusCode
from heron.instance.tests.python.network.mock_generator import MockSTStmgrClient

import heron.instance.tests.python.mock_protobuf as mock_protobuf

class STStmgrClientTest(unittest.TestCase):
  def setUp(self):
    self.stmgr_client = MockSTStmgrClient()

  def tearDown(self):
    self.stmgr_client = None

  def test_on_connect_ok(self):
    self.stmgr_client.on_connect(StatusCode.OK)
    self.assertTrue(self.stmgr_client.register_msg_called)
    # timeout_task should be included in timer_tasks
    self.assertEqual(len(self.stmgr_client.looper.timer_tasks), 1)

  def test_on_connect_error(self):
    self.stmgr_client.on_connect(StatusCode.CONNECT_ERROR)
    self.assertEqual(len(self.stmgr_client.looper.timer_tasks), 1)
    self.assertEqual(self.stmgr_client.looper.timer_tasks[0][1].__name__, 'start_connect')

  def test_on_response(self):
    with self.assertRaises(RuntimeError):
      self.stmgr_client.on_response(StatusCode.INVALID_PACKET, None, None)

    with self.assertRaises(RuntimeError):
      self.stmgr_client.on_response(StatusCode.OK, None, mock_protobuf.get_mock_instance())

    self.stmgr_client.on_response(StatusCode.OK, None, mock_protobuf.get_mock_register_response())
    self.assertTrue(self.stmgr_client.handle_register_response_called)

  def test_on_error(self):
    self.stmgr_client.on_error()
    self.assertEqual(len(self.stmgr_client.looper.timer_tasks), 1)
    self.assertEqual(self.stmgr_client.looper.timer_tasks[0][1].__name__, 'start_connect')
