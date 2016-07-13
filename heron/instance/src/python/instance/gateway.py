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
import asyncore
import threading

from network.stmgr_client import StmgrClient
from heron.common.src.python.log import Log

class Gateway(threading.Thread):
  STMGR_HOST = "127.0.0.1"

  # TODO: gateway looper, out_metrics_queue, system_config not given yet
  def __init__(self, topology_name, topology_id, instance, stream_port, metrics_port, in_stream,
               out_stream, control_stream):
    super(Gateway, self).__init__()
    self._stmgr_client = StmgrClient(self.STMGR_HOST, stream_port, topology_name, topology_id,
                                     instance, in_stream, out_stream, control_stream)

  def run(self):
    self._stmgr_client.start_connect()
    asyncore.loop()


