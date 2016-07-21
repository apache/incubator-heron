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

from heron.instance.src.python.instance.bolt import Bolt
from heron.instance.src.python.misc.communicator import HeronCommunicator
from heron.instance.src.python.misc.outgoing_tuple_helper import OutgoingTupleHelper
from heron.common.src.python.utils.misc import PhysicalPlanHelper, PythonSerializer
from heron.instance.src.python.network.protocol import REQID, HeronProtocol, IncomingPacket, StatusCode
from heron.instance.src.python.network.heron_client import HeronClient

import heron.instance.src.python.network.mock_protobuf as mock_protobuf

from heron.proto import tuple_pb2


prim_list = [1000, -234, 0.00023, "string", ["abc", "def", "ghi"], True, False, ("hello", 123, True), None]

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

# Returns legit looking topology
def get_a_sample_pplan(with_detail=False):
  """Returns a legitimate looking pplan

  This topology has 1 spout and 2 bolts. Currently no input/output streams.
  There is only one stream manager.

  [Instance 1: spout1]
    - instance_id = "instance1"
    - task_id = 100
    - component_index = 0
    - component_name = "spout1"

  [Instance 2: bolt1]
    - instance_id = "instance2"
    - task_id = 200
    - component_index = 0
    - component_name = "bolt1"

  [instance 3: bolt2]
    - instance_id = "instance3"
    - task_id = 300
    - component_index = 0
    - component_name = "bolt2"

  :param with_detail: If False (default), returns just pplan. If True, returns both pplan and a list of dictionaries for each instance containing instance_id, task_id, comp_index, comp_name
  """

  spout_1 = mock_protobuf.get_mock_spout(component=mock_protobuf.get_mock_component(name="spout1",
                                                                                    python_cls="heron.examples.src.python.word_spout.WordSpout"))
  bolt_1 = mock_protobuf.get_mock_bolt(component=mock_protobuf.get_mock_component(name="bolt1"))
  bolt_2 = mock_protobuf.get_mock_bolt(component=mock_protobuf.get_mock_component(name="bolt2"))

  topology = mock_protobuf.get_mock_topology(spouts=[spout_1], bolts=[bolt_1, bolt_2])


  instance_ids = ["instance1", "instance2", "instancer3"]
  task_ids = [100, 200, 300]
  comp_indexes = [0, 0, 0]
  comp_names = ["spout1", "bolt1", "bolt2"]
  instances = []

  for i_id, t_id, c_i, c_name in zip(instance_ids, task_ids, comp_indexes, comp_names):
    info = mock_protobuf.get_mock_instance_info(task_id=t_id, component_index = c_i, component_name=c_name)
    instance = mock_protobuf.get_mock_instance(instance_id=i_id, info=info)
    instances.append(instance)

  pplan = mock_protobuf.get_mock_pplan(topology=topology, instances=instances)

  if not with_detail:
    return pplan
  else:
    keys = ["instance_id", "task_id", "comp_index", "comp_name"]
    zipped = zip(instance_ids, task_ids, comp_indexes, comp_names)
    return pplan, [dict(zip(keys, z)) for z in zipped]

def get_a_sample_register_response():
  """Creates a sample RegisterInstanceResponse based on get_a_sample_pplan()"""
  return mock_protobuf.get_mock_register_response(pplan=get_a_sample_pplan())


def make_data_tuple_from_list(lst, serializer=PythonSerializer()):
  """Make HeronDataTuple from a list of objects"""
  data_tuple = tuple_pb2.HeronDataTuple()
  data_tuple.key = 0

  tuple_size_in_bytes = 0

  for obj in lst:
    serialized = serializer.serialize(obj)
    data_tuple.values.append(serialized)
    tuple_size_in_bytes += len(serialized)
  return data_tuple, tuple_size_in_bytes

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

class MockOutgoingTupleHelper(OutgoingTupleHelper):
  SAMPLE_SUCCESS = 0
  def __init__(self, type=SAMPLE_SUCCESS):
    self.called_init_new_data = False
    self.called_init_new_control = False

    if type == MockOutgoingTupleHelper.SAMPLE_SUCCESS:
      pplan_helper, out_stream = self._prepare_sample_success()
      super(MockOutgoingTupleHelper, self).__init__(pplan_helper, out_stream)

  def _prepare_sample_success(self):
    pplan, instances = get_a_sample_pplan(with_detail=True)
    pplan_helper = PhysicalPlanHelper(pplan, instances[0]["instance_id"])
    out_stream = HeronCommunicator(producer=None, consumer=None)
    return pplan_helper, out_stream

  def _init_new_data_tuple(self, stream_id):
    self.called_init_new_data = True
    OutgoingTupleHelper._init_new_data_tuple(self, stream_id)

  def _init_new_control_tuple(self):
    self.called_init_new_control = True
    OutgoingTupleHelper._init_new_control_tuple(self)

class MockBolt(Bolt):
  SAMPLE_SUCCESS = 0
  def __init__(self, type=SAMPLE_SUCCESS, serializer=PythonSerializer()):
    self.received_data_tuple = None
    if type == MockBolt.SAMPLE_SUCCESS:
      pplan_helper, in_stream, out_stream = self._prepare_sample_success()
      super(MockBolt, self).__init__(pplan_helper, in_stream, out_stream, serializer)

  def _prepare_sample_success(self):
    """Prepare simple successful case -- in_stream and out_stream are the same instance"""
    pplan, instances = get_a_sample_pplan(with_detail=True)
    # Instance 2 and 3 are Bolts
    # let's create bolt for instance 2
    pplan_helper = PhysicalPlanHelper(pplan, instances[1]["instance_id"])
    stream = HeronCommunicator(producer=None, consumer=None)
    in_stream = stream
    out_stream = stream
    return pplan_helper, in_stream, out_stream

  def _handle_data_tuple(self, data_tuple, stream):
    self.received_data_tuple = data_tuple
    Bolt._handle_data_tuple(self, data_tuple, stream)
