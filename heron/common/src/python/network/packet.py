import sys
import struct
from heron.common.src.python.network.reqid import ReqId

class IncomingPacket:
  """
  Takes in serialized bytes and provides methods to deserialize it.
  TODO: The current name is consistent with C++/Java, but maybe we need to
  have a better name for this class
  """
  def __init__(self, data):
    self.data = data
    self.position = 0

  def unpackInt(self, pos):
    # Unpack int from bytes represented in newtork order (big-endian)
    return struct.unpack(">I", self.data[pos: pos+4])[0]

  def unpackTypename(self):
    """
    Get the fully qualified type name of the protobuf message
    """
    size = self.unpackInt(self.position)
    self.position += 4
    typename = self.data[self.position: self.position + size]
    self.position += size
    return typename

  def unpackReqId(self):
    """
    Get the request id for this packet
    """
    reqId = self.data[self.position: self.position + ReqId.size]
    self.position += ReqId.size
    return reqId

  def unpackProtobuf(self, message):
    """
    Deserialize the protobuf message
    """
    messageSize = self.unpackInt(self.position)
    self.position += 4
    message.ParseFromString(self.data[self.position : self.position + messageSize])
    return message

def pack(reqId, message):
  """
  Pack the request Id and the protobuf message according to our protocol
  <overall data size> <typename size> <typename> <reqid> <proto byte size> <serialized proto>
  """

  typename = message.DESCRIPTOR.full_name
  dataSize = 4 + len(typename) + ReqId.size + 4 + message.ByteSize()
  data = struct.pack(">I", dataSize)
  data = data + struct.pack(">I", len(typename))
  data = data + typename
  data = data + reqId
  msgSize = message.ByteSize()
  data = data + struct.pack(">I", msgSize)
  data = data + message.SerializeToString()
  return data
