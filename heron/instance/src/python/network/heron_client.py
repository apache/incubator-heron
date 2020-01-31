#!/usr/bin/env python
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

'''heron_client.py'''

import asyncore
import socket
import time
import traceback

from abc import abstractmethod

from heron.common.src.python.utils.log import Log
import heron.instance.src.python.utils.system_constants as constants
from heron.instance.src.python.network import HeronProtocol, REQID, StatusCode, OutgoingPacket

# pylint: disable=too-many-instance-attributes
# pylint: disable=fixme
class HeronClient(asyncore.dispatcher):
  """Python implementation of HeronClient, using asyncore module"""
  def __init__(self, looper, hostname, port, socket_map, socket_options):
    """Initializes HeronClient

    :type looper: ``GatewayLooper`` (heron.instance.src.python.network)
    :param looper: looper object
    :type hostname: str
    :param hostname: endpoint hostname
    :type port: int
    :param port: endpoint port
    :type socket_map: dict
    :param socket_map: socket map used for asyncore.dispatcher
    :type socket_options: ``SocketOptions`` (heron.common.src.python.network)
    :param socket_options: options for the socket and this client
    """
    asyncore.dispatcher.__init__(self, map=socket_map)
    self.looper = looper
    self.hostname = hostname
    self.port = int(port)
    self.endpoint = (self.hostname, self.port)
    self.out_buffer = []
    self.socket_options = socket_options

    # map <message name -> message.Message object>
    self.registered_message_map = dict()
    self.response_message_map = dict()
    self.context_map = dict()
    self.incomplete_pkt = None

    self.total_bytes_written = 0
    self.total_pkt_written = 0
    self.total_bytes_received = 0
    self.total_pkt_received = 0

    # for compatibility with 2.7.3
    self._connecting = False

    Log.debug("Initializing %s with endpoint: %s, \nsocket_map: %s, \nsocket_options: %s"
              % (self._get_classname(), str(self.endpoint),
                 str(socket_map), str(self.socket_options)))


  ##################################
  # asyncore.dispatcher override
  ##################################

  # called when connect is ready
  def handle_connect(self):
    Log.info("Connected to %s:%d" % (self.hostname, self.port))
    self._connecting = False
    self.on_connect(StatusCode.OK)

  # called when close is ready
  def handle_close(self):
    Log.info("%s: handle_close() called" % self._get_classname())
    self._handle_close()
    self.on_error()

  def _handle_close(self):
    self._clean_up_state()
    self.close()

  def _clean_up_state(self):
    self.out_buffer = []
    self.total_bytes_written = 0
    self.total_pkt_written = 0
    self.total_bytes_received = 0
    self.total_pkt_received = 0

    self.registered_message_map = dict()
    self.response_message_map = dict()
    self.context_map = dict()
    self.incomplete_pkt = None
    self._connecting = False

  # read bytes stream from socket and convert them into a list of IncomingPacket
  def handle_read(self):
    start_cycle_time = time.time()
    bytes_read = 0
    num_pkt_read = 0
    read_pkt_list = []

    read_batch_time_sec = self.socket_options.nw_read_batch_time_ms * constants.MS_TO_SEC
    read_batch_size_bytes = self.socket_options.nw_read_batch_size_bytes

    while (time.time() - start_cycle_time - read_batch_time_sec) < 0 and \
            bytes_read < read_batch_size_bytes:
      if self.incomplete_pkt is None:
        # incomplete packet doesn't exist
        pkt = HeronProtocol.read_new_packet(self)
        pkt.read(self)
      else:
        # continue reading into the incomplete packet
        Log.debug("In handle_read(): Continue reading")
        pkt = self.incomplete_pkt
        pkt.read(self)

      if pkt.is_complete:
        num_pkt_read += 1
        bytes_read += pkt.get_pktsize()
        Log.debug("Read a complete packet of size %d" % bytes_read)
        self.incomplete_pkt = None
        read_pkt_list.append(pkt)
      else:
        Log.debug("In handle_read(): Packet read not yet complete")
        self.incomplete_pkt = pkt
        break

    self.total_bytes_received += bytes_read
    self.total_pkt_received += num_pkt_read

    for pkt in read_pkt_list:
      self._handle_packet(pkt)

  def handle_write(self):
    if len(self.out_buffer) == 0:
      return
    start_cycle_time = time.time()
    bytes_written = 0
    num_pkt_written = 0

    write_batch_time_sec = self.socket_options.nw_write_batch_time_ms * constants.MS_TO_SEC
    write_batch_size_bytes = self.socket_options.nw_write_batch_size_bytes

    while (time.time() - start_cycle_time - write_batch_time_sec) < 0 and \
            bytes_written < write_batch_size_bytes and len(self.out_buffer) > 0:
      outgoing_pkt = self.out_buffer[0]
      outgoing_pkt.send(self)

      if outgoing_pkt.sent_complete:
        num_pkt_written += 1
        bytes_written += len(outgoing_pkt)
        self.out_buffer.remove(outgoing_pkt)
      else:
        # sending this packet not complete yet, will continue later
        break

    self.total_bytes_written += bytes_written
    self.total_pkt_written += num_pkt_written

  def writable(self):
    if self._connecting:
      return True
    return len(self.out_buffer) != 0

  def readable(self):
    return True

  #################################

  def start_connect(self):
    """Tries to connect to the Heron Server

    ``loop()`` method needs to be called after this.
    """
    Log.debug("In start_connect() of %s" % self._get_classname())
    # TODO: specify buffer size, exception handling
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)

    # when ready, handle_connect is called
    self._connecting = True
    self.connect(self.endpoint)

  def stop(self):
    """Disconnects and stops the client"""
    # TODO: cleanup things and close the connection
    self._handle_close()

  def register_on_message(self, msg_builder):
    """Registers protobuf message builders that this client wants to receive

    :param msg_builder: callable to create a protobuf message that this client wants to receive
    """
    message = msg_builder()
    Log.debug("In register_on_message(): %s" % message.DESCRIPTOR.full_name)
    self.registered_message_map[message.DESCRIPTOR.full_name] = msg_builder

  def send_request(self, request, context, response_type, timeout_sec):
    """Sends a request message (REQID is non-zero)"""
    # generates a unique request id
    reqid = REQID.generate()
    Log.debug("%s: In send_request() with REQID: %s" % (self._get_classname(), str(reqid)))
    # register response message type
    self.response_message_map[reqid] = response_type
    self.context_map[reqid] = context

    # Add timeout for this request if necessary
    if timeout_sec > 0:
      def timeout_task():
        self.handle_timeout(reqid)
      self.looper.register_timer_task_in_sec(timeout_task, timeout_sec)

    outgoing_pkt = OutgoingPacket.create_packet(reqid, request)
    self._send_packet(outgoing_pkt)

  def send_message(self, message):
    """Sends a message (REQID is zero)"""
    Log.debug("In send_message() of %s" % self._get_classname())
    outgoing_pkt = OutgoingPacket.create_packet(REQID.generate_zero(), message)
    self._send_packet(outgoing_pkt)

  def handle_timeout(self, reqid):
    """Handles timeout"""
    if reqid in self.context_map:
      context = self.context_map.pop(reqid)
      self.response_message_map.pop(reqid)
      self.on_response(StatusCode.TIMEOUT_ERROR, context, None)

  def handle_error(self):
    _, t, v, tbinfo = asyncore.compact_traceback()

    self_msg = "%s failed for object at %0x" % (self._get_classname(), id(self))
    Log.error("Uncaptured python exception, closing channel %s (%s:%s %s)" %
              (self_msg, t, v, tbinfo))

    if self._connecting:
      # Error when trying to connect
      # first cleanup by handle_close(), and tells a subclass about this error.
      # the subclass can then call start_connect() again, if appropriate
      self._handle_close()
      self.on_connect(StatusCode.CONNECT_ERROR)
    else:
      self._handle_close()
      self.on_error()

  def _handle_packet(self, packet):
    # only called when packet.is_complete is True
    # otherwise, it's just an message -- call on_incoming_message()
    typename, reqid, serialized_msg = HeronProtocol.decode_packet(packet)
    if reqid in self.context_map:
      # this incoming packet has the response of a request
      context = self.context_map.pop(reqid)
      response_msg = self.response_message_map.pop(reqid)

      try:
        response_msg.ParseFromString(serialized_msg)
      except Exception as e:
        Log.error("Invalid Packet Error: %s" % str(e))
        self._handle_close()
        self.on_error()
        return

      if response_msg.IsInitialized():
        self.on_response(StatusCode.OK, context, response_msg)
      else:
        Log.error("Response not initialized")
        self._handle_close()
        self.on_error()
    elif reqid.is_zero():
      # this is a Message -- no need to send back response
      try:
        if typename not in self.registered_message_map:
          raise ValueError("%s is not registered in message map" % typename)
        msg_builder = self.registered_message_map[typename]
        message = msg_builder()
        message.ParseFromString(serialized_msg)
        if message.IsInitialized():
          self.on_incoming_message(message)
        else:
          raise RuntimeError("Message not initialized")
      except Exception as e:
        Log.error("Error when handling message packet: %s" % str(e))
        Log.error(traceback.format_exc())
        raise RuntimeError("Problem reading message")
    else:
      # might be a timeout response
      Log.info("In handle_packet(): Received message whose REQID is not registered: %s"
               % str(reqid))

  def _send_packet(self, pkt):
    """Pushes a packet to a send buffer, the content of which will be send when available"""
    self.out_buffer.append(pkt)

  def _get_classname(self):
    return self.__class__.__name__

  ############################################################
  # Below are the interfaces to be implemented by a subclass #
  ############################################################

  @abstractmethod
  def on_connect(self, status):
    """Called when the client is connected

    Should be implemented by a subclass.
    """
    pass

  @abstractmethod
  def on_response(self, status, context, response):
    """Called when the client receives a response

    Should be implemented by a subclass.
    """
    pass

  @abstractmethod
  def on_incoming_message(self, message):
    """Called when the client receives a message

    Should be implemented by a subclass.
    """
    pass

  @abstractmethod
  def on_error(self):
    """Called when the client meets errors

    Note that this method is not called when a connection is not yet established.
    In such a case, ``on_connect()`` with status == StatusCode.CONNECT_ERROR is called.
    """
    pass
