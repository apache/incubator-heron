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
import socket

from heron.instance.src.python.network.protocol import REQID, HeronProtocol, IncomingPacket, StatusCode
from heron.instance.src.python.network.heron_client import HeronClient
import heron.instance.src.python.network.mock_protobuf as mock_protobuf

def convert_to_incoming_packet(reqid, message):
  raw = HeronProtocol.get_outgoing_packet(reqid, message)
  dispatcher = MockDispatcher()
  dispatcher.prepare_with_raw(raw)
  packet = IncomingPacket()
  packet.read(dispatcher)

  packet.data = packet.data
  return packet

# Returns a list of mock request packets (REQID is non-zero)
def get_mock_requst_packets():
  pkt_list = []
  raw_list = []

  # normal packet (PhysicalPlan as request)
  reqid = REQID.generate()
  message = mock_protobuf.get_mock_register_response()
  normal_pkt = convert_to_incoming_packet(reqid, message)
  pkt_list.append(normal_pkt)
  raw_list.append((reqid, message))

  return pkt_list, raw_list

# Returns a mock request packet and its raw data
def get_a_mock_request_packet_and_raw():
  reqid = REQID.generate()
  message = mock_protobuf.get_mock_register_response()
  pkt = convert_to_incoming_packet(reqid, message)
  return pkt, reqid, message

# Returns a list of mock message packets and its builder (REQID is zero)
def get_a_mock_message_list_and_builder():
  pkt_list = []
  raw_list = mock_protobuf.get_many_mock_pplans()

  for msg in raw_list:
    reqid = REQID.generate_zero()
    pkt = convert_to_incoming_packet(reqid, msg)
    pkt_list.append(pkt)
    typename = msg.DESCRIPTOR.full_name

  builder, typename = mock_protobuf.get_pplan_builder_and_typename()
  return pkt_list, raw_list, builder, typename

# Returns an incomplete packet
def get_fail_packet():
  raw = HeronProtocol.get_outgoing_packet(REQID.generate(), mock_protobuf.get_mock_pplan())
  packet = IncomingPacket.create_packet(raw[:4], raw[4:])
  packet.is_complete = False
  return packet

class MockDispatcher:
  PARTIAL_DATA_SIZE = 4
  def __init__(self):
    self.to_be_received = ""
    self.eagain_test = False
    self.fatal_error_test = False

  def prepare_with_raw(self, raw):
    self.to_be_received = raw

  def prepare_normal(self):
    #self.to_be_received = b"".join(pkt.convert_to_raw() for pkt in get_mock_packets()[0])
    for pkt in get_mock_requst_packets()[0]:
      self.to_be_received += pkt.convert_to_raw()

  def prepare_header_only(self):
    pkt = get_mock_requst_packets()[0][0]
    self.to_be_received = pkt.header

  def prepare_partial_data(self):
    pkt = get_mock_requst_packets()[0][0]
    self.to_be_received = pkt.header + pkt.data[:self.PARTIAL_DATA_SIZE]

  def prepare_eagain(self):
    self.eagain_test = True

  def prepare_fatal(self):
    self.fatal_error_test = True

  def recv(self, numbytes):
    if self.fatal_error_test:
      raise RuntimeError("Fatal Error Test")
    elif self.eagain_test:
      raise socket.error, (socket.errno.EAGAIN, "EAGAIN Test")

    ret = self.to_be_received[:numbytes]
    self.to_be_received = self.to_be_received[numbytes:]
    return ret

class MockHeronClient(HeronClient):
  HOST = '127.0.0.1'
  PORT = 9090
  def __init__(self):
    HeronClient.__init__(self, self.HOST, self.PORT)
    self.passed_on_connect = False
    self.on_response_status = None
    self.called_handle_packet = False
    self.dispatcher = MockDispatcher()
    self.incoming_msg = None

  def on_connect(self, status):
    if status == StatusCode.OK:
      self.passed_on_connect = True

  def on_response(self, status, context, response):
    self.on_response_status = status

  def on_incoming_message(self, message):
    self.incoming_msg = message

  def recv(self, numbytes):
    return self.dispatcher.recv(numbytes)

  def handle_packet(self, packet):
    # should only be called when packet is complete
    self.called_handle_packet = True
    HeronClient.handle_packet(self, packet)
