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

'''Unittest for EventLooper'''
import time
import unittest

from heron.instance.src.python.network import EventLooper

# pylint: disable=missing-docstring
# pylint: disable=W0212
class EventLooperTest(unittest.TestCase):
  def setUp(self):
    self.looper = EventLooper()
    self.global_value = 6

  def tearDown(self):
    self.looper = None

  def test_loop(self):
    def to_run():
      for _ in range(3):
        self.global_value += 10
        self.looper.wake_up()
      self.looper.exit_loop()

    self.looper.add_wakeup_task(to_run)
    self.looper.loop()
    self.assertEqual(36, self.global_value)

  def test_add_wakeup_task(self):
    def to_run():
      self.looper.exit_loop()
      self.global_value = 10

    self.looper.add_wakeup_task(to_run)
    self.looper.loop()
    self.assertEqual(10, self.global_value)

  def test_register_timer_task(self):
    def to_run():
      self.looper.exit_loop()
      self.global_value = 10

    start_time = time.time()
    interval = 1
    self.looper.register_timer_task_in_sec(to_run, interval)
    self.looper.loop()
    end_time = time.time()
    self.assertAlmostEqual(start_time + interval, end_time, delta=0.05)
    self.assertEqual(10, self.global_value)

  def test_exit_loop(self):
    def to_run():
      self.looper.exit_loop()
      self.global_value = 10

    self.looper.add_wakeup_task(to_run)
    self.looper.loop()
    self.assertEqual(10, self.global_value)

  def test_exit_tasks(self):
    def exit_task():
      self.global_value = 10

    def to_run():
      self.looper.add_exit_task(exit_task)
      self.assertEqual(6, self.global_value)
      self.looper.exit_loop()

    self.looper.add_wakeup_task(to_run)
    self.looper.loop()
    self.assertEqual(10, self.global_value)

  def test_get_next_timeout_interval(self):
    to_run = lambda: None

    interval = 1.0
    self.looper.register_timer_task_in_sec(to_run, interval)
    next_interval = self.looper._get_next_timeout_interval()
    self.assertAlmostEqual(next_interval, interval, delta=0.01)

  def test_run_once(self):
    def to_run():
      self.global_value = 10

    self.looper.add_wakeup_task(to_run)
    self.looper._run_once()
    self.assertEqual(10, self.global_value)

  def test_trigger_timers(self):
    def to_run():
      self.global_value = 10

    interval = 1.0
    self.looper.register_timer_task_in_sec(to_run, interval)

    # run right now, to_run() should not have been executed yet
    self.looper._trigger_timers()
    self.assertEqual(6, self.global_value)

    time.sleep(interval)
    self.looper._trigger_timers()
    self.looper._trigger_timers()
    self.assertEqual(10, self.global_value)
