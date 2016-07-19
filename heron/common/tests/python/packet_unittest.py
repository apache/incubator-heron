import unittest2 as unittest
from mock import call, patch, Mock
from heron.common.src.python.network import packet
from heron.common.src.python.network.packet import IncomingPacket
from heron.common.src.python.network.reqid import ReqId
from heron.proto.stmgr_pb2 import RegisterInstanceRequest
from heron.proto.physical_plan_pb2 import Instance, InstanceInfo

class PacketTest(unittest.TestCase):
  def setUp(self):
    iinfo = InstanceInfo()
    iinfo.task_id = 2
    iinfo.component_index = 0
    iinfo.component_name = "exclaim1"

    instance = Instance()
    instance.info.MergeFrom(iinfo)
    instance.instance_id = "container_1_exclaim1_2"
    instance.stmgr_id = "stmgr-1"

    self.registerRequestProto = RegisterInstanceRequest()
    self.registerRequestProto.instance.MergeFrom(instance)
    self.registerRequestProto.topology_name = "AckingTopology"
    self.registerRequestProto.topology_id = "AckingTopologye1609c00-b289-4e3a-b6f4-f046ad2eae36"
    self.reqId = ReqId.generate()

  def test_pack_unpack(self):
    serData = packet.pack(self.reqId, self.registerRequestProto)

    incomingPacket = IncomingPacket(serData[4:])

    typename = incomingPacket.unpackTypename()
    self.assertEqual(typename, self.registerRequestProto.DESCRIPTOR.full_name)

    reqId = incomingPacket.unpackReqId()
    self.assertEqual(reqId, self.reqId)

    deserializedProto = incomingPacket.unpackProtobuf(RegisterInstanceRequest())

    self.assertEqual(deserializedProto.topology_id, self.registerRequestProto.topology_id)
    self.assertEqual(deserializedProto.topology_name, self.registerRequestProto.topology_name)
    self.assertEqual(deserializedProto.instance.instance_id, self.registerRequestProto.instance.instance_id)

  # TODO: Add more unit tests

