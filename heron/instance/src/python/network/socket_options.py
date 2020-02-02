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

'''socket_options.py'''

from collections import namedtuple

from heron.common.src.python.utils.log import Log
import heron.instance.src.python.utils.system_constants as const
from heron.instance.src.python.utils import system_config

SocketOptions = namedtuple('Options', 'nw_write_batch_size_bytes, nw_write_batch_time_ms, '
                                      'nw_read_batch_size_bytes, nw_read_batch_time_ms, '
                                      'sock_send_buf_size_bytes, sock_recv_buf_size_bytes')

def create_socket_options():
  """Creates SocketOptions object from a given sys_config dict"""
  sys_config = system_config.get_sys_config()
  opt_list = [const.INSTANCE_NETWORK_WRITE_BATCH_SIZE_BYTES,
              const.INSTANCE_NETWORK_WRITE_BATCH_TIME_MS,
              const.INSTANCE_NETWORK_READ_BATCH_SIZE_BYTES,
              const.INSTANCE_NETWORK_READ_BATCH_TIME_MS,
              const.INSTANCE_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES,
              const.INSTANCE_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES]

  Log.debug("In create_socket_options()")
  try:
    value_lst = [int(sys_config[opt]) for opt in opt_list]
    sock_opt = SocketOptions(*value_lst)
    return sock_opt
  except ValueError as e:
    # couldn't convert to int
    raise ValueError("Invalid value in sys_config: %s" % str(e))
  except KeyError as e:
    # option key was not found
    raise KeyError("Incomplete sys_config: %s" % str(e))
