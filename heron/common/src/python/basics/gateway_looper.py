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
import os
import time
import select

import errno

from event_looper import EventLooper
from heron.common.src.python.color import Log
import asyncore

class GatewayLooper(EventLooper):
  def __init__(self):
    super(GatewayLooper, self).__init__()
    self.map = None

    # Pipe used for wake up select
    self.pipe_r, self.pipe_w = os.pipe()

    self.register_timer_task_in_sec(self.exit_loop, 5)
    self.started = time.asctime()
    def at_exit():
      os.close(self.pipe_r)
      os.close(self.pipe_w)
      Log.info("GatewayLooper - started: " + str(self.started) + ", stopped: " + str(time.asctime()))
    self.add_exit_task(at_exit)

  def prepare_map(self):
    # TODO need to change and use signal.set_wakeup_fd
    self.map = asyncore.socket_map

  def do_wait(self):
    next_timeout = self.get_next_timeout_interval()
    if next_timeout > 0:
      self.poll(timeout=next_timeout, map=self.map)
    else:
      self.poll(timeout=0.0, map=self.map)

  def wake_up(self):
    os.write(self.pipe_w, "\n")

  def poll(self, timeout=0.0, map=None):
    if map:
      r = []; w = []; e = []
      for fd, obj in map.items():
        is_r = obj.readable()
        is_w = obj.writable()
        if is_r:
          r.append(fd)
        # accepting sockets should not be writable
        if is_w and not obj.accepting:
          w.append(fd)
        if is_r or is_w:
          e.append(fd)

      # Add wakeup fd
      r.append(self.pipe_r)

      try:
        r, w, e = select.select(r, w, e, timeout)
      except select.error, err:
        if err.args[0] != errno.EINTR:
          raise
        else:
          return

      for fd in r:
        obj = map.get(fd)
        if obj is None:
          continue
        asyncore.read(obj)

      for fd in w:
        obj = map.get(fd)
        if obj is None:
          continue
        asyncore.write(obj)

      for fd in e:
        obj = map.get(fd)
        if obj is None:
          continue
        asyncore._exception(obj)

