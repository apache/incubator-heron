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
import socket
from network.protocol import HeronProtocol, REQID, IncomingPacket
from heron.proto import stmgr_pb2, physical_plan_pb2, topology_pb2
from utils import Log
from google.protobuf.descriptor import Descriptor

class HeronTestHandler(asyncore.dispatcher_with_send):
  def __init__(self, sock):
    asyncore.dispatcher_with_send.__init__(self, sock)
    self.incomplete_pkt = None
    self.out_buffer = ''

  def handle_read(self):
    # TODO: currently only reads one packet

    if self.incomplete_pkt is None:
      # incomplete packet doesn't exist
      pkt = HeronProtocol.read_new_packet(self)
    else:
      # continue reading into the incomplete packet
      pkt = self.incomplete_pkt
      pkt.read(self)

    if pkt.is_complete:
      self.incomplete_pkt = None
      self.handle_packet(pkt)
    else:
      self.incomplete_pkt = pkt

  def handle_packet(self, packet):
    typename, reqid, serialized_msg = HeronProtocol.decode_packet(packet)
    print typename
    print reqid

    # make sure this is right class
    
    request = stmgr_pb2.RegisterInstanceRequest()
    request.ParseFromString(serialized_msg)
    
    print request.__str__()

    self.send_response(reqid)

  def send_response(self, reqid):
    # create NewInstanceAssignmentMessage
    response = self.get_mock_assignment_message()
    pkt = HeronProtocol.get_outgoing_packet(reqid, response)
    self.send_packet(pkt)

  def send_packet(self, pkt):
    self.out_buffer += pkt

  def handle_write(self):
    sent = self.send(self.out_buffer)
    self.out_buffer = self.out_buffer[sent:]

  def get_mock_assignment_message(self):

    # topology
    sample_topology = topology_pb2.Topology()
    sample_topology.id = "topology_id"
    sample_topology.name = "topology_name"
    sample_topology.state = 1

    # instance info
    instance_info = physical_plan_pb2.InstanceInfo()
    instance_info.task_id = 123
    instance_info.component_index = 23
    instance_info.component_name = "hello"

    # pplan
    sample_pplan = physical_plan_pb2.PhysicalPlan()
    sample_pplan.topology.MergeFrom(sample_topology)

    sample_stmgr = sample_pplan.stmgrs.add()
    sample_stmgr.id = "Stmgr_id"
    sample_stmgr.host_name = "localhost"
    sample_stmgr.data_port = 9999
    sample_stmgr.local_endpoint = "hello"

    sample_instance = sample_pplan.instances.add()
    sample_instance.instance_id = "instance_id_is_this"
    sample_instance.stmgr_id = "stmgr_id_is_this"
    sample_instance.info.MergeFrom(instance_info)

    # message
    mock_message = stmgr_pb2.NewInstanceAssignmentMessage()
    mock_message.pplan.MergeFrom(sample_pplan)

    return mock_message




class HeronTestServer(asyncore.dispatcher):
  def __init__(self, host, port):
    asyncore.dispatcher.__init__(self)
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.set_reuse_addr()
    self.bind((host, port))
    self.listen(1)

  def handle_accept(self):
    pair = self.accept()
    if pair is not None:
      sock, addr = pair
      print "Incoming connection from %s" % repr(addr)
      handler = HeronTestHandler(sock)

server = HeronTestServer('localhost', 8080)
asyncore.loop()

