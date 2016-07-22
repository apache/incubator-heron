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

import heron.instance.tests.python.mock_generator as mock
from heron.common.src.python.log import Log
from heron.proto import stmgr_pb2

from heron.common.src.python.network import HeronProtocol


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
    Log.debug("In handle_read() with typename: " + typename + ", and reqid: " + str(reqid))

    if reqid.is_zero():
      Log.info("Received a new message")
      # Should be TupleMessage instance
      message = stmgr_pb2.TupleMessage()
      message.ParseFromString(serialized_msg)
      Log.debug("Received message: \n" + str(message))
    else:
      Log.info("Received a register request")

      # make sure this is right class
      request = stmgr_pb2.RegisterInstanceRequest()
      request.ParseFromString(serialized_msg)

      Log.debug("Request message: \n" + str(request))

      self.send_response(reqid)

  def send_response(self, reqid):
    # create NewInstanceAssignmentMessage
    Log.debug("Sending response...")
    response = mock.get_a_sample_register_response()
    pkt = HeronProtocol.get_outgoing_packet(reqid, response)
    self.send_packet(pkt)

  def send_packet(self, pkt):
    self.out_buffer += pkt

  def handle_write(self):
    sent = self.send(self.out_buffer)
    self.out_buffer = self.out_buffer[sent:]


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
      Log.info("Incoming connection from %s" % repr(addr))
      handler = HeronTestHandler(sock)

server = HeronTestServer('localhost', 8080)
asyncore.loop()

