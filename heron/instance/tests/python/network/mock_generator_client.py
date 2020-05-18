#!/usr/bin/env python3
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

"""module for generating mock classes/objects for testing purposes"""
import socket

from heron.instance.src.python.network import (REQID, OutgoingPacket,
                                               IncomingPacket, StatusCode,
                                               SocketOptions)
from heron.instance.src.python.network import HeronClient
import heron.instance.tests.python.mock_protobuf as mock_protobuf

# pylint: disable=missing-docstring
# pylint: disable=unused-argument

# Python's primitive type list
prim_list = [1000, -234, 0.00023, "string",
             ["abc", "def", "ghi"], True, False,
             ("tuple", 123, True), None, {}]

def convert_to_incoming_packet(reqid, message):
  """Convert (reqid, message) pair to IncomingPacket object"""
  raw = OutgoingPacket.create_packet(reqid, message).raw
  dispatcher = MockDispatcher()
  dispatcher.prepare_with_raw(raw)
  packet = IncomingPacket()
  packet.read(dispatcher)

  packet.data = packet.data
  return packet

# Returns a list of mock request packets (REQID is non-zero)
def get_mock_requst_packets(is_message):
  """Returns a list of valid IncomingPackets with non-zero REQID and the corresponding raw data"""
  pkt_list = []
  raw_list = []
  message_list = [mock_protobuf.get_mock_register_response(),
                  mock_protobuf.get_mock_config(),
                  mock_protobuf.get_mock_bolt(),
                  mock_protobuf.get_mock_topology()]

  # normal packet (PhysicalPlan as request)
  for message in message_list:
    if is_message:
      reqid = REQID.generate_zero()
    else:
      reqid = REQID.generate()
    normal_pkt = convert_to_incoming_packet(reqid, message)
    pkt_list.append(normal_pkt)
    raw_list.append((reqid, message))

  return pkt_list, raw_list

def get_a_mock_request_packet_and_raw():
  """Returns a tuple of mock (IncomingPacket, REQID, RegisterResponse message)"""
  reqid = REQID.generate()
  message = mock_protobuf.get_mock_register_response()
  pkt = convert_to_incoming_packet(reqid, message)
  return pkt, reqid, message

def get_a_mock_message_list_and_builder():
  """Get a list of IncomingPackets, the corresponding mock messages, builder and typename"""
  pkt_list = []
  raw_list = mock_protobuf.get_many_mock_pplans()

  for msg in raw_list:
    reqid = REQID.generate_zero()
    pkt = convert_to_incoming_packet(reqid, msg)
    pkt_list.append(pkt)

  builder, typename = mock_protobuf.get_pplan_builder_and_typename()
  return pkt_list, raw_list, builder, typename

def get_fail_packet():
  """Returns an incomplete IncomingPacket object"""
  raw = OutgoingPacket.create_packet(REQID.generate(), mock_protobuf.get_mock_pplan()).raw
  packet = IncomingPacket.create_packet(raw[:4], raw[4:])
  packet.is_complete = False
  return packet

class MockDispatcher:
  """Mock asyncore.dispatcher class, supporting only recv() and send() method

  This dispatcher provides several options as to how to prepare its receive buffer.
  """
  PARTIAL_DATA_SIZE = 4
  def __init__(self):
    self.to_be_received = b""
    self.eagain_test = False
    self.fatal_error_test = False

  def prepare_with_raw(self, raw):
    """the content of ``raw`` will be prepared in the recv buffer"""
    self.to_be_received = raw

  def prepare_valid_response(self):
    """a number of valid packets will be prepared in the recv buffer"""
    pkt_list, _ = get_mock_requst_packets(is_message=False)
    for pkt in pkt_list:
      self.to_be_received += pkt.convert_to_raw()

  def prepare_header_only(self):
    """a packet with just a header (incomplete packet) will be prepared in the recv buffer"""
    pkt = get_mock_requst_packets(is_message=False)[0][0]
    self.to_be_received = pkt.header

  def prepare_partial_data(self):
    """a partial data packet will be prepared in the recv buffer"""
    pkt = get_mock_requst_packets(is_message=False)[0][0]
    self.to_be_received = pkt.header + pkt.data[:self.PARTIAL_DATA_SIZE]

  def prepare_eagain(self):
    """prepare so that EAGAIN error is raised when recv() is called """
    self.eagain_test = True

  def prepare_fatal(self):
    """prepare so that RuntimeError is raised when recv() is called"""
    self.fatal_error_test = True

  def recv(self, numbytes):
    """reads ``numbytes`` from the recv buffer"""
    if self.fatal_error_test:
      raise RuntimeError("Fatal Error Test")
    elif self.eagain_test:
      raise socket.error(socket.errno.EAGAIN, "EAGAIN Test")

    ret = self.to_be_received[:numbytes]
    self.to_be_received = self.to_be_received[numbytes:]
    return ret

  # pylint: disable=no-self-use
  def send(self, buf):
    """mock sends the content of a given buffer"""
    return len(buf)

class MockHeronClient(HeronClient):
  HOST = '127.0.0.1'
  PORT = 9090
  def __init__(self):
    socket_options = SocketOptions(32768, 16, 32768, 16, 1024000, 1024000)
    HeronClient.__init__(self, None, self.HOST, self.PORT, {}, socket_options)
    self.on_connect_called = False
    self.on_response_status = None
    self.called_handle_packet = False
    self.dispatcher = MockDispatcher()
    self.incoming_msg = None
    self.on_error_called = False

  def on_connect(self, status):
    if status == StatusCode.OK:
      self.on_connect_called = True

  def on_response(self, status, context, response):
    self.on_response_status = status

  def on_error(self):
    self.on_error_called = True

  def on_incoming_message(self, message):
    self.incoming_msg = message

  def recv(self, numbytes):
    return self.dispatcher.recv(numbytes)

  def _handle_packet(self, packet):
    # should only be called when packet is complete
    self.called_handle_packet = True
    HeronClient._handle_packet(self, packet)

  def _handle_close(self):
    pass
