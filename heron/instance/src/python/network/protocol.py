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

# Implementation of Heron's application level protocol for Python
# Hugely referenced from com.twitter.heron.common.network
import struct
import random
import socket

from heron.common.src.python.color import Log
from google.protobuf.message import Message


class HeronProtocol(object):
  # TODO: check endian
  INT_PACK_FMT = ">I"
  HEADER_SIZE = 4

  @staticmethod
  def pack_int(i):
    return struct.pack(HeronProtocol.INT_PACK_FMT, i)

  @staticmethod
  def unpack_int(i):
    return struct.unpack(HeronProtocol.INT_PACK_FMT, i)[0]

  @staticmethod
  def _get_size_to_pack_string(string):
    return 4 + len(string)

  @staticmethod
  def _get_size_to_pack_message(message):
    return 4 + message.ByteSize()

  @staticmethod
  def get_outgoing_packet(reqid, message):
    Log.debug("In get_outgoing_pkt():\n" + message.__str__())
    assert message.IsInitialized()
    packet = ""

    # calculate the totla size of the packet incl. header
    typename = message.DESCRIPTOR.full_name

    datasize = HeronProtocol._get_size_to_pack_string(typename) + \
               REQID.REQID_SIZE + HeronProtocol._get_size_to_pack_message(message)
    Log.debug("Outgoing datasize: " + str(datasize))

    # first write out how much data is there as the header
    packet += HeronProtocol.pack_int(datasize)

    # next write the type string
    packet += HeronProtocol.pack_int(len(typename))
    packet += typename

    # reqid
    packet += reqid.pack()

    # add the proto
    packet += HeronProtocol.pack_int(message.ByteSize())
    packet += message.SerializeToString()
    return str(packet)

  @staticmethod
  def read_new_packet(dispatcher):
    packet = IncomingPacket()
    packet.read(dispatcher)
    return packet

  @staticmethod
  def decode_packet(packet):
    if not packet.is_complete:
      raise RuntimeError("In decode_packet(): Packet corrupted")

    data = packet.data

    len_typename = HeronProtocol.unpack_int(data[:4])
    data = data[4:]

    typename = data[:len_typename]
    data = data[len_typename:]

    reqid = REQID.unpack(data[:REQID.REQID_SIZE])
    data = data[REQID.REQID_SIZE:]

    len_msg = HeronProtocol.unpack_int(data[:4])
    data = data[4:]

    serialized_msg = data[:len_msg]

    return typename, reqid, serialized_msg

class IncomingPacket(object):
  def __init__(self):
    self.header = ''
    self.data = ''
    self.is_header_read = False
    self.is_complete = False
    # for debugging identification purposes
    self.id = random.getrandbits(8)

  @staticmethod
  def create_packet(header, data):
    packet = IncomingPacket()
    packet.header = header
    packet.data = data

    if len(header) == HeronProtocol.HEADER_SIZE:
      packet.is_header_read = True
      if len(data) == packet.get_datasize():
        packet.is_complete = True

    return packet

  def convert_to_raw(self):
    return self.header + self.data

  def get_datasize(self):
    if not self.is_header_read:
      return -1
    return HeronProtocol.unpack_int(self.header)

  def read(self, dispatcher):
    try:
      if not self.is_header_read:
        # try reading header
        to_read = HeronProtocol.HEADER_SIZE - len(self.header)
        self.header += dispatcher.recv(to_read)
        if len(self.header) == HeronProtocol.HEADER_SIZE:
          self.is_header_read = True
        else:
          Log.debug("Header read incomplete: " + str(len(self.header)))
          return

      if self.is_header_read and not self.is_complete:
        # try reading data
        to_read = self.get_datasize() - len(self.data)
        self.data += dispatcher.recv(to_read)
        if len(self.data) == self.get_datasize():
          self.is_complete = True
    except socket.error as e:
      if e.errno == socket.errno.EAGAIN or e.errno == socket.errno.EWOULDBLOCK:
        # Try again later -> call continue_read later
        Log.debug("Try again error")
        pass
      else:
        # Fatal error
        Log.debug("Fatal error")
        raise RuntimeError("Fatal error occured in IncomingPacket.read()")

  def __str__(self):
    return "Packet ID: " + str(self.id) + ", header: " + \
           repr(self.is_header_read) + ", complete: " + repr(self.is_complete)


class REQID(object):
  REQID_SIZE = 32

  def __init__(self, data_bytes):
    self.bytes = data_bytes

  @staticmethod
  def generate():
    data_bytes = bytearray(random.getrandbits(8) for i in range (REQID.REQID_SIZE))
    return REQID(data_bytes)

  @staticmethod
  def generate_zero():
    data_bytes = bytearray(0 for i in range (REQID.REQID_SIZE))
    return REQID(data_bytes)

  def pack(self):
    return self.bytes

  def is_zero(self):
    return self == REQID.generate_zero()

  @staticmethod
  def unpack(raw_data):
    return REQID(bytearray(raw_data))

  def __eq__(self, another):
    return hasattr(another, 'bytes') and self.bytes == another.bytes

  def __hash__(self):
    return hash(self.__str__())

  def __str__(self):
    if self.is_zero():
      return "ZERO"
    else:
      return ''.join([str(i) for i in list(self.bytes)])

class StatusCode:
  OK = 0
  WRITE_ERROR = 1
  READ_ERROR = 2
  INVALID_PACKET = 3
  CONNECT_ERROR = 4
  CLOSE_ERROR = 5
  TIMEOUT_ERROR = 6

