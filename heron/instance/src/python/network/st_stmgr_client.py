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
'''Stream Manager client for single-thread heron instance in python'''
from heron.common.src.python.config import system_config
from heron.common.src.python.utils.log import Log
from heron.common.src.python.utils.misc import PhysicalPlanHelper
from heron.common.src.python.network import HeronClient, StatusCode
from heron.proto import common_pb2, stmgr_pb2, tuple_pb2

import heron.common.src.python.constants as constants

# pylint: disable=too-many-arguments
class SingleThreadStmgrClient(HeronClient):
  """SingleThreadStmgrClient is a Heron Client that communicates with Stream Manager

  This class is intended to be used with SingleThreadHeronInstance.

  It will:
  1. Register the message of NewInstanceAssignmentMessage and TupleMessage
  2. Send a register request when on_connect() is called
  3. Handle relative response for requests
  """
  def __init__(self, looper, heron_instance_cls, strmgr_host, port, topology_name, topology_id,
               instance, sock_map, gateway_metrics, socket_options):
    HeronClient.__init__(self, looper, strmgr_host, port, sock_map, socket_options)
    self.heron_instance_cls = heron_instance_cls
    self.topology_name = topology_name
    self.topology_id = topology_id
    # physical_plan_pb2.Instance message
    self.instance = instance
    self.gateway_metrics = gateway_metrics
    self._pplan_helper = None
    self.sys_config = system_config.get_sys_config()

  # send register request
  def on_connect(self, status):
    Log.debug("In on_connect of STStmgrClient")
    if status != StatusCode.OK:
      Log.error("Error connecting to Stream Manager with status: %s", str(status))
      retry_interval = float(self.sys_config[constants.INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC])
      self.looper.register_timer_task_in_sec(self.start_connect, retry_interval)
      return
    self._register_msg_to_handle()
    self._send_register_req()

  def on_response(self, status, context, response):
    Log.debug("In on_response with status: %s, with context: %s", str(status), str(context))
    if status != StatusCode.OK:
      raise RuntimeError("Response from Stream Manager not OK")
    if isinstance(response, stmgr_pb2.RegisterInstanceResponse):
      self._handle_register_response(response)
    else:
      Log.error("Unknown kind of response received: %s", response.DESCRIPTOR.full_name)
      raise RuntimeError("Unknown kind of response received from Stream Manager")

  def on_incoming_message(self, message):
    self.gateway_metrics.update_received_packet(message.ByteSize())
    if isinstance(message, stmgr_pb2.NewInstanceAssignmentMessage):
      Log.info("Handling assignment message from direct NewInstanceAssignmentMessage")
      self._handle_assignment_message(message.pplan)
    elif isinstance(message, stmgr_pb2.TupleMessage):
      self._handle_new_tuples(message)
    elif isinstance(message, tuple_pb2.HeronTupleSet2):
      self._handle_new_tuples_2(message)
    else:
      raise RuntimeError("Unknown kind of message received from Stream Manager")

  def on_error(self):
    Log.error("Disconnected from Stream Manager")
    # cleaning up
    self._pplan_helper = None
    # retry again
    self.on_connect(StatusCode.CONNECT_ERROR)

  def _register_msg_to_handle(self):
    # pylint: disable=unnecessary-lambda
    new_instance_builder = lambda: stmgr_pb2.NewInstanceAssignmentMessage()
    tuple_msg_builder = lambda: stmgr_pb2.TupleMessage()
    hts2_msg_builder = lambda: tuple_pb2.HeronTupleSet2()
    self.register_on_message(new_instance_builder)
    self.register_on_message(tuple_msg_builder)
    self.register_on_message(hts2_msg_builder)

  def _send_register_req(self):
    request = stmgr_pb2.RegisterInstanceRequest()
    request.instance.MergeFrom(self.instance)
    request.topology_name = self.topology_name
    request.topology_id = self.topology_id

    timeout_sec = float(self.sys_config[constants.INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC])

    self.send_request(request, "Context", stmgr_pb2.RegisterInstanceResponse(), timeout_sec)

  def _handle_register_response(self, response):
    """Called when a register response (RegisterInstanceResponse) arrives"""
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
    self.heron_instance_cls.handle_new_tuple_set(tuple_msg.set)

  def _handle_new_tuples_2(self, hts2):
    """Called when new HeronTupleSet2 arrives
       Strange that we are not using TupleMessage2
       but to keep consistency with Java part we use HeronTupleSet2
    """
    self.heron_instance_cls.handle_new_tuple_set_2(hts2)

  def _handle_assignment_message(self, pplan):
    """Called when new NewInstanceAssignmentMessage arrives"""
    Log.debug("In handle_assignment_message() of STStmgrClient, Physical Plan: \n%s", str(pplan))
    new_helper = PhysicalPlanHelper(pplan, self.instance.instance_id,
                                    self.heron_instance_cls.topo_pex_file_abs_path)

    if self._pplan_helper is not None and \
      (self._pplan_helper.my_component_name != new_helper.my_component_name or
       self._pplan_helper.my_task_id != new_helper.my_task_id):
      raise RuntimeError("Our Assignment has changed. We will die to pick it.")

    if self._pplan_helper is None:
      Log.info("Received a new Physical Plan")
      Log.info("Push the new pplan_helper to Heron Instance")
      self.heron_instance_cls.handle_assignment_msg(new_helper)
    else:
      Log.info("Received a new Physical Plan with the same assignment -- State Change")
      Log.info("Old state: %s, new state: %s.",
               self._pplan_helper.get_topology_state(), new_helper.get_topology_state())
      self.heron_instance_cls.handle_state_change_msg(new_helper)

    self._pplan_helper = new_helper
