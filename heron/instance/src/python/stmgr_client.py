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
from heron_client import HeronClient
from utils import Log
from heron.proto import stmgr_pb2
from heron.proto import physical_plan_pb2

class StmgrClient(HeronClient):
  def __init__(self, strmgr_host, port, topology_name, topology_id,
               instance, in_stream_queue, out_stream_queue, in_control_queue):
    HeronClient.__init__(self, strmgr_host, port)
    self.topology_name = topology_name
    self.topology_id = topology_id
    self.instance = instance

  # send register request
  def on_connect(self):
    Log.debug("In on_connect of " + self.get_classname())
    self.register_msg_to_handle()
    self.send_register_req()

  def register_msg_to_handle(self):
    self.register_on_message(stmgr_pb2.NewInstanceAssignmentMessage)
    self.register_on_message(stmgr_pb2.TupleMessage)

  def send_register_req(self):
    # TODO: change it to RegisterInstanceRequest
    request = stmgr_pb2.StrMgrHelloRequest()
    request.topology_name = self.topology_name
    request.topology_id = self.topology_id
    request.stmgr = "stmgr1"

    self.send_request(request, None, None, 10)