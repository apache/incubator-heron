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

'''event_looper.py'''
import time
import traceback
import sys

from abc import abstractmethod
from heapq import heappush, heappop

from heron.common.src.python.utils.log import Log

class EventLooper(object):
  """EventLooper is a Python implementation of WakeableLooper.java

  EventLooper is a class for scheduling recurring tasks that could:

  - Block the thread when ``do_wait()`` is called
  - Unblock it when ``wake_up()`` is called or the waiting time exceeds the timeout
  - Execute timer event

  The EventLooper will execute in a while loop, unless ``exit_loop()`` is called.
  In every execution, it will execute ``_run_once()``, inside which it will:

  - ``do_wait()`` to perform blocking tasks, which will be waken up if ``wake_up()`` is called, or
    time exceeds the timeout, or an event is successfully dispatched.
  - run ``_execute_wakeup_tasks()``, in which registered wakeup tasks are executed. Note that these
    tasks will be executed every loop after wakeup.
  - run ``_trigger_timers()``, in which expired timers are executed and removed.

  Note that the EventLooper class is NOT designed to be thread-safe,
  except for ``wake_up()`` method.
  """
  def __init__(self):
    self.should_exit = False
    self.wakeup_tasks = []
    self.timer_tasks = []
    self.exit_tasks = []

  def loop(self):
    """Start loop

    This will start a while loop until ``exit_loop()`` is called.
    """
    while not self.should_exit:
      self._run_once()

    self.on_exit()

  def _run_once(self):
    """Run once, should be called only from loop()"""
    try:
      self.do_wait()
      self._execute_wakeup_tasks()
      self._trigger_timers()
    except Exception as e:
      Log.error("Error occured during _run_once(): " + str(e))
      Log.error(traceback.format_exc())
      self.should_exit = True

  def on_exit(self):
    """Called when exiting"""
    Log.info("In on_exit() of event_looper")
    for task in self.exit_tasks:
      task()

  @abstractmethod
  def do_wait(self):
    """Blocking operation, should be implemented by a subclass"""
    pass

  @abstractmethod
  def wake_up(self):
    """Wakes up do_wait() operation, should be implemented by a subclass

    Note that this method should be implemented in a thread-safe way.
    """
    pass

  def add_wakeup_task(self, task):
    """Add a wakeup task

    :param task: function to be run as a wakeup task
    """
    self.wakeup_tasks.append(task)
    # make sure to run this at least once
    self.wake_up()

  def add_exit_task(self, task):
    """Add an exit task

    :param task: function to be run as an exit task
    """
    self.exit_tasks.append(task)

  def register_timer_task_in_sec(self, task, second):
    """Registers a new timer task

    :param task: function to be run at a specified second from now
    :param second: how many seconds to wait before the timer is triggered
    """
    # Python time is in float
    second_in_float = float(second)
    expiration = time.time() + second_in_float
    heappush(self.timer_tasks, (expiration, task))

  def exit_loop(self):
    """Exits the loop"""
    self.should_exit = True
    self.wake_up()

  def _get_next_timeout_interval(self):
    """Get the next timeout from now

    This should be used from do_wait().
    :returns (float) next_timeout, or 10.0 if there are no timer events
    """
    if len(self.timer_tasks) == 0:
      return sys.maxsize
    else:
      next_timeout_interval = self.timer_tasks[0][0] - time.time()
      return next_timeout_interval

  def _execute_wakeup_tasks(self):
    """Executes wakeup tasks, should only be called from loop()"""
    # Check the length of wakeup tasks first to avoid concurrent issues
    size = len(self.wakeup_tasks)
    for i in range(size):
      self.wakeup_tasks[i]()

  def _trigger_timers(self):
    """Triggers expired timers"""
    current = time.time()
    while len(self.timer_tasks) > 0 and (self.timer_tasks[0][0] - current <= 0):
      task = heappop(self.timer_tasks)[1]
      task()
