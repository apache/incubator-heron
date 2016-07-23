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
from heron.common.src.python.log import Log
from heron.proto import stmgr_pb2, common_pb2

from heron.common.src.python.utils.misc import PhysicalPlanHelper
from heron.common.src.python.network import HeronClient, StatusCode

# SingleThreadStmgrClient is an implementation of the Heron client in python and communicates
# with Stream Manager. It will:
# 1. Register the message of NewInstanceAssignmentMessage and TupleMessage
# 2. Send a register request when on_connect() is called
# 3. Handle relative response for requests
# TODO: will implement the rest later
class SingleThreadStmgrClient(HeronClient):
  """SingleThreadStmgrClient is a Stream Manager Client for a single-threaded Heron Instance"""
  def __init__(self, looper, heron_instance_cls, strmgr_host, port, topology_name, topology_id,
               instance, sock_map, gateway_metrics, socket_options):
    HeronClient.__init__(self, looper, strmgr_host, port, sock_map, socket_options)
    self.heron_instance_cls = heron_instance_cls
    self.topology_name = topology_name
    self.topology_id = topology_id
    self.instance = instance
    self.gateway_metrics = gateway_metrics

    self._pplan_helper = None

  # send register request
  def on_connect(self, status):
    Log.debug("In on_connect of STStmgrClient")
    self._register_msg_to_handle()
    self._send_register_req()

  def on_response(self, status, context, response):
    Log.debug("In on_response with status: " + str(status))
    if status != StatusCode.OK:
      raise RuntimeError("Response from Stream Manager not OK")
    if isinstance(response, stmgr_pb2.RegisterInstanceResponse):
      self._handle_register_response(response)
    else:
      Log.error("Weird kind: " + response.DESCRIPTOR.full_name)
      raise RuntimeError("Unknown kind of response received from Stream Manager")

  def on_incoming_message(self, message):
    self.gateway_metrics.received_packet(message.ByteSize())

    if isinstance(message, stmgr_pb2.NewInstanceAssignmentMessage):
      Log.info("Handling assignment message from direct NewInstanceAssignmentMessage")
      self._handle_assignment_message(message.pplan)
    elif isinstance(message, stmgr_pb2.TupleMessage):
      self._handle_new_tuples(message)
    else:
      raise RuntimeError("Unknown kind of message received from Stream Manager")

  def _register_msg_to_handle(self):
    new_instance_builder = lambda : stmgr_pb2.NewInstanceAssignmentMessage()
    tuple_msg_builder = lambda : stmgr_pb2.TupleMessage()
    self.register_on_message(new_instance_builder)
    self.register_on_message(tuple_msg_builder)

  def _send_register_req(self):
    request = stmgr_pb2.RegisterInstanceRequest()
    request.instance.MergeFrom(self.instance)
    request.topology_name = self.topology_name
    request.topology_id = self.topology_id

    self.send_request(request, "Context", stmgr_pb2.RegisterInstanceResponse(), 10)

  def _handle_register_response(self, response):
    """Called when a register response (RegisterInstanceResponse) arrives"""
    Log.debug("In _handle_register_response()")
    if response.status.status != common_pb2.StatusCode.Value("OK"):
      raise RuntimeError("Stream Manager returned a not OK response for register")
    Log.info("We registered ourselves to the Stream Manager")

    if response.HasField("pplan"):
      Log.info("Handling assignment message from response")
      self._handle_assignment_message(response.pplan)
    else:
      Log.debug("Received a register response with no pplan")

  def _handle_new_tuples(self, tuple_msg):
    """Called when new TupleMessage arrives"""
    #self._in_stream.offer(tuple_msg.set)

    self.heron_instance_cls.handle_new_tuple_set(tuple_msg.set)

  def _handle_assignment_message(self, pplan):
    """Called when new NewInstanceAssignmentMessage arrives"""
    Log.info("In _handle_assignment_message(): Physical Plan: \n" + str(pplan))
    new_helper = PhysicalPlanHelper(pplan, self.instance.instance_id)

    # TODO: handle when pplan_helper already exists

    if self._pplan_helper is None:
      Log.info("Received a new Physical Plan")
    else:
      Log.info("Received a new Physical Plan with the same assignment -- State Change")

    self._pplan_helper = new_helper

    Log.info("Push to Slave")
    #self._control_stream.offer(new_helper)
    self.heron_instance_cls.handle_assignment_msg(new_helper)
