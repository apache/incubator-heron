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
'''socket_options.py'''

from collections import namedtuple
from heron.common.src.python.log import Log
import heron.common.src.python.constants as const

SocketOptions = namedtuple('Options', 'nw_write_batch_size_bytes, nw_write_batch_time_ms, '
                                      'nw_read_batch_size_bytes, nw_read_batch_time_ms, '
                                      'sock_send_buf_size_bytes, sock_recv_buf_size_bytes')


def create_socket_options(sys_config):
  """Creates SocketOptions object from a given sys_config dict"""
  opt_list = [const.INSTANCE_NW_WRITE_BATCH_SIZE_BYTES,
              const.INSTANCE_NW_WRITE_BATCH_TIME_MS,
              const.INSTANCE_NW_READ_BATCH_SIZE_BYTES,
              const.INSTANCE_NW_READ_BATCH_TIME_MS,
              const.INSTANCE_NW_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES,
              const.INSTANCE_NW_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES]

  Log.debug("In create_socket_options()")
  try:
    value_lst = [int(sys_config[opt]) for opt in opt_list]
    sock_opt = SocketOptions(*value_lst)
    Log.debug("Socket options: " + str(sock_opt))
    return sock_opt
  except ValueError as e:
    # couldn't convert to int
    raise ValueError("Invalid value in sys_config: " + e.message)
  except KeyError as e:
    # option key was not found
    raise KeyError("Incomplete sys_config: " + e.message)
