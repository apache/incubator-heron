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

'''Unittest for GatewayLooper'''
import threading
import time
import unittest

from heron.instance.src.python.network.gateway_looper import GatewayLooper

# pylint: disable=missing-docstring
class GatewayLooperTest(unittest.TestCase):
  def setUp(self):
    pass

  @staticmethod
  def sleep_and_call_wakeup(sec, looper):
    # Meant to be a target of another thread
    time.sleep(sec)
    # add_wakeup_task() calls wakeup()
    looper.wake_up()

  def test_wakeup(self):
    sleep_times = [0.1, 0.3, 0.5, 1.0, 3.0, 5.0]
    for sleep in sleep_times:
      start, end = self.prepare_wakeup_test(sleep)
      self.assertAlmostEqual(start + sleep, end, delta=0.05)

  def prepare_wakeup_test(self, sleep, poll_timeout=30.0):
    looper = GatewayLooper(socket_map={})
    waker = threading.Thread(target=self.sleep_and_call_wakeup, args=(sleep, looper))
    waker.start()

    start_time = time.time()
    # Wait in poll() for 30 sec or waken up
    looper.poll(timeout=poll_timeout)
    end_time = time.time()
    return start_time, end_time
