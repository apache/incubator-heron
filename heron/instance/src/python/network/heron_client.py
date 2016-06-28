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

# TODO: write unit test

import asyncore
import socket

from abc import abstractmethod

from google.protobuf import message
from protocol import HeronProtocol, REQID, IncomingPacket, StatusCode
from heron.common.src.python.color import Log

class HeronClient(asyncore.dispatcher):
  TIMEOUT_SEC = 30.0
  def __init__(self, hostname, port):
    asyncore.dispatcher.__init__(self)
    self.hostname = hostname
    self.port = int(port)
    self.out_buffer = ''
    # name to message.Message
    self.registered_message_map = dict()
    self.response_message_map = dict()
    self.context_map = dict()
    self.incomplete_pkt = None

  ##################################
  # asyncore.dispatcher override
  ##################################

  # called when connect is ready
  def handle_connect(self):
    Log.info("Connected to " + self.hostname + ":" + str(self.port))
    self.on_connect(StatusCode.OK)

  # called when close is ready
  def handle_close(self):
    self.close()

  # read bytes stream from socket and convert them into a list of IncomingPacket
  def handle_read(self):
    # TODO: currently only reads one packet
    if self.incomplete_pkt is None:
      # incomplete packet doesn't exist
      pkt = HeronProtocol.read_new_packet(self)
    else:
      # continue reading into the incomplete packet
      Log.debug("In handle_read(): Continue reading")
      pkt = self.incomplete_pkt
      pkt.read(self)

    if pkt.is_complete:
      self.incomplete_pkt = None
      self.handle_packet(pkt)
    else:
      Log.debug("In handle_read(): Packet read not yet complete")
      self.incomplete_pkt = pkt

  def handle_write(self):
    sent = self.send(self.out_buffer)
    self.out_buffer = self.out_buffer[sent:]

  # called when an error was raised
  #def handle_error(self):
  #  Log.debug("Error occurred -- connection closed")
  #  self.stop()

  #################################

  def start(self):
    # TODO: specify buffer size, exception handling
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)

    # when ready, handle_connect is called
    self.connect((self.hostname, self.port))
    asyncore.loop(timeout=self.TIMEOUT_SEC)

  def stop(self):
    # TODO: cleanup things and close the connection
    self.handle_close()

  # Register the protobuf Message's name with protobuf Message
  def register_on_message(self, message):
    Log.debug("In register_on_message(): " + message.DESCRIPTOR.full_name)
    self.registered_message_map[message.DESCRIPTOR.full_name] = message

  def send_request(self, request, context, response_type, timeout_sec):
    # TODO: send request and implement timeout handler
    # generates a unique request id
    reqid = REQID.generate()
    Log.debug("In send_request() with REQID: " + str(reqid))
    # register response message type
    self.response_message_map[reqid] = response_type
    self.context_map[reqid] = context

    pkt = HeronProtocol.get_outgoing_packet(reqid, request)
    self.send_packet(pkt)

  def send_message(self):
    # TODO: send message (non-request-response based communication)
    pass

  def handle_timeout(self):
    pass

  def handle_packet(self, packet):
    # TODO: check if it has REQID: if yes, handle response message -- call on_reseponse()
    # only called when packet.is_complete is True
    # otherwise, it's just an message -- call on_incoming_message()
    typename, reqid, serialized_msg = HeronProtocol.decode_packet(packet)
    if self.context_map.has_key(reqid):
      # this incoming packet has the response of a request
      context = self.context_map.pop(reqid)
      response_msg = self.response_message_map.pop(reqid)

      try:
        response_msg.ParseFromString(serialized_msg)
        if response_msg.IsInitialized():
          Log.debug("In handle_Packet(): Received response with size " +
                    str(packet.get_datasize()) +
                    "\n" + response_msg.__str__())
          self.on_response(StatusCode.OK, context, response_msg)
        else:
          raise message.DecodeError
      except message.DecodeError:
        self.on_response(StatusCode.INVALID_PACKET, context, None)

  def send_packet(self, pkt):
    self.out_buffer += pkt

  def get_classname(self):
    return self.__class__.__name__

  #######################################################
  # Below are the interfaces to be implemented by child #
  #######################################################

  @abstractmethod
  def on_connect(self, status):
    pass

  @abstractmethod
  def on_response(self, status, context, response):
    pass
