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
from heron.common.src.python.color import Log
from heron.proto import stmgr_pb2
from heron.proto import physical_plan_pb2

from protocol import StatusCode

# StmgrClient is an implementation of the Heron client in python and communicates
# with Stream Manager. It will:
# 1. Register the message of NewInstanceAssignmentMessage and TupleMessage
# 2. Send a register request when on_connect() is called
# 3. Handle relative response for requests
# TODO: will implement the rest later

class StmgrClient(HeronClient):

  def __init__(self, strmgr_host, port, topology_name, topology_id,
               instance, in_stream_queue, out_stream_queue, in_control_queue):
    HeronClient.__init__(self, strmgr_host, port)
    self.topology_name = topology_name
    self.topology_id = topology_id
    self.instance = instance

  # send register request
  def on_connect(self, status):
    Log.debug("In on_connect of " + self.get_classname())
    self.register_msg_to_handle()
    self.send_register_req()

  def on_response(self, status, context, response):
    if status != StatusCode.OK:
      raise RuntimeError("Response from Stream Manager not OK")
    # TODO: use of isinstance -- check later if appropriate
    if isinstance(response, stmgr_pb2.RegisterInstanceResponse):
      self.handle_register_response(response)
    else:
      Log.debug("Weird kind: " + response.DESCRIPTOR.full_name)
      raise RuntimeError("Unknown kind of response received from Stream Manager")

  def register_msg_to_handle(self):
    self.register_on_message(stmgr_pb2.NewInstanceAssignmentMessage)
    self.register_on_message(stmgr_pb2.TupleMessage)

  def send_register_req(self):
    # TODO: change it to RegisterInstanceRequest
    request = stmgr_pb2.RegisterInstanceRequest()
    request.instance.MergeFrom(self.instance)
    request.topology_name = self.topology_name
    request.topology_id = self.topology_id

    self.send_request(request, "Context", stmgr_pb2.RegisterInstanceResponse(), 10)

  def handle_register_response(self, response):
    Log.debug("In handle_register_response()")

