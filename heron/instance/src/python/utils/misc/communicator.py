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

'''communicator.py: module responsible for communication between Python heron modules'''
import sys
from queue import Queue, Full, Empty

from heron.common.src.python.utils.log import Log

class HeronCommunicator(object):
  """HeronCommunicator: a wrapper class for non-blocking queue in Heron.

  Note that this class does not yet implement the dynamic tuning of expected available capacity,
  as it is not necessary for single thread instance.
  """
  def __init__(self, producer_cb=None, consumer_cb=None):
    """Initialize HeronCommunicator

    :param producer_cb: Callback function to be called (usually on producer thread)
           when ``poll()`` is called by the consumer. Default ``None``
    :param consumer_cb: Callback function to be called (usually on consumer thread)
           when ``offer()`` is called by the producer. Default ``None``
    """
    self._producer_callback = producer_cb
    self._consumer_callback = consumer_cb
    self._buffer = Queue()
    self.capacity = sys.maxsize

  def register_capacity(self, capacity):
    """Registers the capacity of this communicator

    By default, the capacity of HeronCommunicator is set to be ``sys.maxsize``
    """
    self.capacity = capacity

  def get_available_capacity(self):
    return max(self.capacity - self.get_size(), 0)

  def get_size(self):
    """Returns the size of the buffer"""
    return self._buffer.qsize()

  def is_empty(self):
    """Returns whether the buffer is empty"""
    return self._buffer.empty()

  def poll(self):
    """Poll from the buffer

    It is a non-blocking operation, and when the buffer is empty, it raises Queue.Empty exception
    """
    try:
      # non-blocking
      ret = self._buffer.get(block=False)
      if self._producer_callback is not None:
        self._producer_callback()
      return ret
    except Empty:
      Log.debug("%s: Empty in poll()" % str(self))
      raise Empty

  def offer(self, item):
    """Offer to the buffer

    It is a non-blocking operation, and when the buffer is full, it raises Queue.Full exception
    """
    try:
      # non-blocking
      self._buffer.put(item, block=False)
      if self._consumer_callback is not None:
        self._consumer_callback()
      return True
    except Full:
      Log.debug("%s: Full in offer()" % str(self))
      raise Full

  def clear(self):
    """Clear the buffer"""
    while not self.is_empty():
      self.poll()

  def __str__(self):
    return "HeronCommunicator"
