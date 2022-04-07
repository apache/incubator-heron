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

"""Implementation of Heron's application level protocol for Python"""
import random
import socket
import struct

from heron.common.src.python.utils.log import Log

class HeronProtocol:
  """Heron's application level network protocol"""
  INT_PACK_FMT = ">I"
  HEADER_SIZE = 4

  @staticmethod
  def pack_int(i):
    """Packs int to bytestring"""
    return struct.pack(HeronProtocol.INT_PACK_FMT, i)

  @staticmethod
  def unpack_int(i):
    """Unpacks bytestring to int"""
    return struct.unpack(HeronProtocol.INT_PACK_FMT, i)[0]

  @staticmethod
  def get_size_to_pack_string(string):
    """Get size to pack string, four byte used for specifying length of the string"""
    return 4 + len(string)

  @staticmethod
  def get_size_to_pack_message(message):
    """Get size to pack message, four byte used for specifying length of the message"""
    return 4 + message.ByteSize()

  @staticmethod
  def read_new_packet(dispatcher):
    """Reads new packet and returns an IncomingPacket object

    :param dispatcher: asyncore's dispatcher class from which packets is read
    """
    packet = IncomingPacket()
    packet.read(dispatcher)
    return packet

  @staticmethod
  def decode_packet(packet):
    """Decodes an IncomingPacket object and returns (typename, reqid, serialized message)"""
    if not packet.is_complete:
      raise RuntimeError("In decode_packet(): Packet corrupted")

    data = packet.data

    len_typename = HeronProtocol.unpack_int(data[:4])
    data = data[4:]

    typename = data[:len_typename].decode()
    data = data[len_typename:]

    reqid = REQID.unpack(data[:REQID.REQID_SIZE])
    data = data[REQID.REQID_SIZE:]

    len_msg = HeronProtocol.unpack_int(data[:4])
    data = data[4:]

    serialized_msg = data[:len_msg]

    return typename, reqid, serialized_msg

class OutgoingPacket:
  """Wrapper class for outgoing packet"""
  def __init__(self, raw_data):
    self.raw = bytes(raw_data)
    self.to_send = bytes(raw_data)

  def __len__(self):
    return len(self.raw)

  @staticmethod
  def create_packet(reqid, message):
    """Creates Outgoing Packet from a given reqid and message

    :param reqid: REQID object
    :param message: protocol buffer object
    """
    assert message.IsInitialized()
    packet = b''

    # calculate the totla size of the packet incl. header
    typename = message.DESCRIPTOR.full_name

    datasize = HeronProtocol.get_size_to_pack_string(typename) + \
               REQID.REQID_SIZE + HeronProtocol.get_size_to_pack_message(message)

    # first write out how much data is there as the header
    packet += HeronProtocol.pack_int(datasize)

    # next write the type string
    packet += HeronProtocol.pack_int(len(typename))
    packet += typename.encode()

    # reqid
    packet += reqid.pack()

    # add the proto
    packet += HeronProtocol.pack_int(message.ByteSize())
    packet += message.SerializeToString()
    return OutgoingPacket(packet)

  @property
  def sent_complete(self):
    """Indicates whether this packet is successfully sent"""
    return len(self.to_send) == 0

  def send(self, dispatcher):
    """Sends this outgoing packet to dispatcher's socket"""
    if self.sent_complete:
      return

    sent = dispatcher.send(self.to_send)
    self.to_send = self.to_send[sent:]

class IncomingPacket:
  """Helper class for incoming packet"""
  def __init__(self):
    """Initializes IncomingPacket object"""
    self.header = b''
    self.data = b''
    self.is_header_read = False
    self.is_complete = False
    # for debugging identification purposes
    self.id = random.getrandbits(8)

  @staticmethod
  def create_packet(header, data):
    """Creates an IncomingPacket object from header and data

    This method is for testing purposes
    """
    packet = IncomingPacket()
    packet.header = header
    packet.data = data

    if len(header) == HeronProtocol.HEADER_SIZE:
      packet.is_header_read = True
      if len(data) == packet.get_datasize():
        packet.is_complete = True

    return packet

  def convert_to_raw(self):
    """Converts this IncomingPacket object to raw string

    This method is for testing purposes
    """
    return self.header + self.data

  def get_datasize(self):
    """Returns the datasize of the packet

    :returns: (int) datasize of the packet, or -1 if header is incomplete
    """
    if not self.is_header_read:
      return -1
    return HeronProtocol.unpack_int(self.header)

  def get_pktsize(self):
    """Returns the size of this packet, including header and data"""
    return len(self.data) + len(self.header)

  def read(self, dispatcher):
    """Reads incoming data from asyncore.dispatcher"""
    try:
      if not self.is_header_read:
        # try reading header
        to_read = HeronProtocol.HEADER_SIZE - len(self.header)
        self.header += dispatcher.recv(to_read)
        if len(self.header) == HeronProtocol.HEADER_SIZE:
          self.is_header_read = True
        else:
          Log.debug("Header read incomplete; read %d bytes of header", len(self.header))
          return

      if self.is_header_read and not self.is_complete:
        # try reading data
        to_read = self.get_datasize() - len(self.data)
        self.data += dispatcher.recv(to_read)
        if len(self.data) == self.get_datasize():
          self.is_complete = True
    except socket.error as e:
      if e.errno in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
        # Try again later -> call continue_read later
        Log.debug("Try again error")
      else:
        # Fatal error
        Log.debug("Fatal error when reading IncomingPacket")
        raise RuntimeError("Fatal error occurred in IncomingPacket.read()") from e

  def __str__(self):
    return f"Packet ID: {str(self.id)}, header: {self.is_header_read}, complete: {self.is_complete}"


class REQID:
  """Helper class for REQID"""
  REQID_SIZE = 32

  def __init__(self, data_bytes):
    self.bytes = data_bytes

  @staticmethod
  def generate():
    """Generates a random REQID for request"""
    data_bytes = bytearray(random.getrandbits(8) for i in range(REQID.REQID_SIZE))
    return REQID(data_bytes)

  @staticmethod
  def generate_zero():
    """Generates a zero REQID for message"""
    data_bytes = bytearray(0 for i in range(REQID.REQID_SIZE))
    return REQID(data_bytes)

  def pack(self):
    """Packs this REQID to bytestring"""
    return self.bytes

  def is_zero(self):
    """Checks if this REQID is zero"""
    return self == REQID.generate_zero()

  @staticmethod
  def unpack(raw_data):
    """Unpacks a given bytestring and returns REQID object"""
    return REQID(bytearray(raw_data))

  def __eq__(self, another):
    return hasattr(another, 'bytes') and self.bytes == another.bytes

  def __hash__(self):
    return hash(self.__str__())

  def __str__(self):
    if self.is_zero():
      return "ZERO"
    return ''.join([str(i) for i in list(self.bytes)])

class StatusCode:
  """StatusCode for Response"""
  OK = 0
  WRITE_ERROR = 1
  READ_ERROR = 2
  INVALID_PACKET = 3
  CONNECT_ERROR = 4
  CLOSE_ERROR = 5
  TIMEOUT_ERROR = 6
