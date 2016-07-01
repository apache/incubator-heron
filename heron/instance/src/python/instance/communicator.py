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
  def __init__(self, producer=None, consumer=None):
    self._producer = producer
    self._consumer = consumer
    self._buffer = Queue.Queue()

  def get_size(self):
    return self._buffer.qsize()

  def is_empty(self):
    return self._buffer.empty()

  def poll(self):
    try:
      # non-blocking
      ret = self._buffer.get(block=False)
      # TODO: wakeup _producer
      return ret
    except Queue.Empty:
      Log.debug(str(self) + " : " + "Empty in poll()")
      # TODO: maybe sleep _consumer?
      raise Queue.Empty

  def offer(self, item):
    try:
      # non-blocking
      self._buffer.put(item, block=False)
      # TODO: wakeup _consumer
      return True
    except Queue.Full:
      Log.debug(str(self) + " : " + "Full in offer()")
      # TODO: maybe sleep _producer?
      return False

  def __str__(self):
    return "HeronCommunicator"




