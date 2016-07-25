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
'''gateway_looper.py'''

import asyncore
import errno
import os
import time
import select

from event_looper import EventLooper
from heron.common.src.python.log import Log

class GatewayLooper(EventLooper):
  """A GatewayLooper, inheriting EventLooper

  It is a class wrapping Python's asyncore module (and selector) to dispatch events.
  This class can be used as a looper for an ``asyncore.dispatcher`` class, instead of calling
  ``asyncore.loop()``.

  As it is a subclass of EventLooper, it will execute in a while loop until
  the ``exit_loop()`` is called.

  In order to use this class, users first need to specify a socket map that maps from
  a file descriptor to ``asyncore.dispatcher`` class, using ``prepare_map()`` method.
  The GatewayLooper will dispatch ready events that are in the specified map.
  """
  def __init__(self):
    super(GatewayLooper, self).__init__()
    self.sock_map = None

    # Pipe used for wake up select
    self.pipe_r, self.pipe_w = os.pipe()

    self.started = time.time()
    Log.debug("Gateway Looper started time: " + str(time.asctime()))

  def prepare_map(self, sock_map):
    """Specifies which socket map to use; should be asyncore's socket map"""
    self.sock_map = sock_map

  def do_wait(self):
    next_timeout = self.get_next_timeout_interval()
    if next_timeout > 0:
      self.poll(timeout=next_timeout)
    else:
      self.poll(timeout=0.0)

  def wake_up(self):
    os.write(self.pipe_w, "\n")
    Log.debug("Wake up called")

  def on_exit(self):
    super(GatewayLooper, self).on_exit()
    os.close(self.pipe_r)
    os.close(self.pipe_w)

  # pylint: disable=too-many-branches
  def poll(self, timeout=0.0):
    """Modified version of poll() from asyncore module"""
    if self.sock_map is None:
      Log.warning("Socket map is not registered to Gateway Looper")
    r = []
    w = []
    e = []

    if self.sock_map is not None:
      for fd, obj in self.sock_map.items():
        is_r = obj.readable()
        is_w = obj.writable()
        if is_r:
          r.append(fd)
        if is_w and not obj.accepting:
          w.append(fd)
        if is_r or is_w:
          e.append(fd)

    # Add wakeup fd
    r.append(self.pipe_r)

    Log.debug("Will select() with timeout: " + str(timeout) + ", with map: " + str(self.sock_map))
    try:
      r, w, e = select.select(r, w, e, timeout)
    except select.error, err:
      Log.debug("Trivial error: " + err.message)
      if err.args[0] != errno.EINTR:
        raise
      else:
        return
    Log.debug("Selected [r]: " + str(r) + " [w]: " + str(w) + " [e]: " + str(e))

    if self.pipe_r in r:
      Log.debug("Read from pipe")
      os.read(self.pipe_r, 1024)
      r.remove(self.pipe_r)

    if self.sock_map is not None:
      for fd in r:
        obj = self.sock_map.get(fd)
        if obj is None:
          continue
        asyncore.read(obj)

      for fd in w:
        obj = self.sock_map.get(fd)
        if obj is None:
          continue
        asyncore.write(obj)

      for fd in e:
        obj = self.sock_map.get(fd)
        if obj is None:
          continue
        # pylint: disable=W0212
        asyncore._exception(obj)
