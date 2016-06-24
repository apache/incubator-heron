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
# Hugely referenced from com.twitter.heron.common.network.OutgoingPacket and REQID
import struct
import random

from heron.instance.src.python.utils import Log
from google.protobuf.message import Message

# TODO: check endian
INT_PACK_FMT = ">i"

def pack_int(i):
  return struct.pack(INT_PACK_FMT, i)

def get_size_to_pack_string(string):
  return 4 + len(string)

def get_size_to_pack_message(serialized_msg):
  return 4 + len(serialized_msg)

def get_outgoing_packet(reqid, message):
  assert message.IsInitialized()
  packet = ""

  # calculate the totla size of the packet incl. header
  typename = message.DESCRIPTOR.full_name

  serialized_msg = message.SerializeToString()

  datasize = get_size_to_pack_string(typename) + \
             REQID.REQID_SIZE + get_size_to_pack_message(serialized_msg)

  # first write out how much data is there as the header
  packet += pack_int(datasize)

  # next write the type string
  packet += pack_int(len(typename))
  packet += bytearray(typename)

  # reqid
  packet += REQID.pack(reqid)

  # add the proto
  packet += pack_int(get_size_to_pack_message(serialized_msg))
  packet += serialized_msg
  return packet

class REQID:
  REQID_SIZE = 32

  def __init__(self, data_bytes):
    self.bytes = data_bytes

  @staticmethod
  def generate():
    data_bytes = bytearray(random.getrandbits(8) for i in range (REQID.REQID_SIZE))
    return REQID(data_bytes)

  @staticmethod
  def pack(reqid):
    return reqid.bytes
