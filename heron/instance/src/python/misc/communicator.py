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
import Queue

from heron.common.src.python.color import Log

class HeronCommunicator(object):
  def __init__(self, producer_cb=None, consumer_cb=None):
    """Initialize HeronCommunicator

    :param producer_cb: Callback function to be called (usually on producer thread) when ``poll()`` is called by the consumer.
    :param consumer_cb: Callback function to be called (usually on consumer thread) when ``offer()`` is called by the producer.
    """
    self._producer_callback = producer_cb
    self._consumer_callback = consumer_cb
    self._buffer = Queue.Queue()

  def get_size(self):
    return self._buffer.qsize()

  def is_empty(self):
    return self._buffer.empty()

  def poll(self):
    try:
      # non-blocking
      ret = self._buffer.get(block=False)
      return ret
    except Queue.Empty:
      Log.debug(str(self) + " : " + "Empty in poll()")
      # TODO: maybe sleep _consumer_callback?
      raise Queue.Empty

  def offer(self, item):
    try:
      # non-blocking
      self._buffer.put(item, block=False)
      return True
    except Queue.Full:
      Log.debug(str(self) + " : " + "Full in offer()")
      # TODO: maybe sleep _producer_callback?
      return False

  def __str__(self):
    return "HeronCommunicator"
