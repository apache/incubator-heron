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

'''gateway_looper.py'''

import asyncore
import errno
import os
import time
import select

from heron.common.src.python.utils.log import Log
from .event_looper import EventLooper


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
  def __init__(self, socket_map):
    """Initializes a GatewayLooper instance

    :param socket_map: socket map used for asyncore.dispatcher
    """
    super(GatewayLooper, self).__init__()
    self.sock_map = socket_map

    # Pipe used for wake up select
    self.pipe_r, self.pipe_w = os.pipe()

    self.started = time.time()
    Log.debug("Gateway Looper started time: " + str(time.asctime()))

  def do_wait(self):
    next_timeout = self._get_next_timeout_interval()
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
    readable_lst = []
    writable_lst = []
    error_lst = []

    if self.sock_map is not None:
      for fd, obj in list(self.sock_map.items()):
        is_r = obj.readable()
        is_w = obj.writable()
        if is_r:
          readable_lst.append(fd)
        if is_w and not obj.accepting:
          writable_lst.append(fd)
        if is_r or is_w:
          error_lst.append(fd)

    # Add wakeup fd
    readable_lst.append(self.pipe_r)

    Log.debug("Will select() with timeout: " + str(timeout) + ", with map: " + str(self.sock_map))
    try:
      readable_lst, writable_lst, error_lst = \
        select.select(readable_lst, writable_lst, error_lst, timeout)
    except select.error as err:
      Log.debug("Trivial error: " + str(err))
      if err.args[0] != errno.EINTR:
        raise
      else:
        return
    Log.debug("Selected [r]: " + str(readable_lst) +
              " [w]: " + str(writable_lst) + " [e]: " + str(error_lst))

    if self.pipe_r in readable_lst:
      Log.debug("Read from pipe")
      os.read(self.pipe_r, 1024)
      readable_lst.remove(self.pipe_r)

    if self.sock_map is not None:
      for fd in readable_lst:
        obj = self.sock_map.get(fd)
        if obj is None:
          continue
        asyncore.read(obj)

      for fd in writable_lst:
        obj = self.sock_map.get(fd)
        if obj is None:
          continue
        asyncore.write(obj)

      for fd in error_lst:
        obj = self.sock_map.get(fd)
        if obj is None:
          continue
        # pylint: disable=W0212
        asyncore._exception(obj)
