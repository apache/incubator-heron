''' mock_proto.py '''
from heron.common.src.python import constants
import heron.proto.execution_state_pb2 as protoEState
import heron.proto.physical_plan_pb2 as protoPPlan
import heron.proto.tmaster_pb2 as protoTmaster
import heron.proto.topology_pb2 as protoTopology

# pylint: disable=no-self-use, missing-docstring
class MockProto(object):
  ''' Mocking Proto'''
  topology_name = "mock_topology_name"
  topology_id = "mock_topology_id"
  cluster = "mock_topology_cluster"
  environ = "mock_topology_environ"

  def create_mock_spout(self,
                        spout_name,
                        output_streams,
                        spout_parallelism):
    spout = protoTopology.Spout()
    spout.comp.name = spout_name
    kv = spout.comp.config.kvs.add()
    kv.key = constants.TOPOLOGY_COMPONENT_PARALLELISM
    kv.type = protoTopology.ConfigValueType.Value('STRING_VALUE')
    kv.value = str(spout_parallelism)
    for stream in output_streams:
      spout.outputs.add().stream.CopyFrom(stream)
    return spout

  def create_mock_bolt(self,
                       bolt_name,
                       input_streams,
                       output_streams,
                       bolt_parallelism):
    bolt = protoTopology.Bolt()
    bolt.comp.name = bolt_name
    kv = bolt.comp.config.kvs.add()
    kv.key = constants.TOPOLOGY_COMPONENT_PARALLELISM
    kv.type = protoTopology.ConfigValueType.Value('STRING_VALUE')
    kv.value = str(bolt_parallelism)
    for stream in input_streams:
      bolt.inputs.add().stream.CopyFrom(stream)
    for stream in output_streams:
      bolt.outputs.add().stream.CopyFrom(stream)
    return bolt

  def create_mock_simple_topology(
      self,
      spout_parallelism=1,
      bolt_parallelism=1):
    """
    Simple topology contains one spout and one bolt.
    """
    topology = protoTopology.Topology()
    topology.id = MockProto.topology_id
    topology.name = MockProto.topology_name

    # Stream1
    stream1 = protoTopology.StreamId()
    stream1.id = "mock_stream1"
    stream1.component_name = "mock_spout"

    # Spout1
    spout = self.create_mock_spout("mock_spout", [stream1], spout_parallelism)
    topology.spouts.extend([spout])

    # Bolt1
    bolt = self.create_mock_bolt("mock_bolt", [stream1], [], bolt_parallelism)
    topology.bolts.extend([bolt])

    return topology

  def create_mock_medium_topology(
      self,
      spout_parallelism=1,
      bolt1_parallelism=1,
      bolt2_parallelism=1,
      bolt3_parallelism=1):
    """
    Medium topology is a three stage topology
    with one spout, two mid stage bolts, and one
    last stage bolt.
    S -str1-> B1 -str3-> B3
    S -str2-> B2 -str4-> B3
    """
    topology = protoTopology.Topology()
    topology.id = "mock_topology_id"
    topology.name = "mock_topology_name"

    # Streams
    stream1 = protoTopology.StreamId()
    stream1.id = "mock_stream1"
    stream1.component_name = "mock_spout1"

    stream2 = protoTopology.StreamId()
    stream2.id = "mock_stream2"
    stream2.component_name = "mock_spout1"

    stream3 = protoTopology.StreamId()
    stream3.id = "mock_stream3"
    stream3.component_name = "mock_bolt1"

    stream4 = protoTopology.StreamId()
    stream4.id = "mock_stream4"
    stream4.component_name = "mock_bolt2"

    # Spouts
    spout1 = self.create_mock_spout("mock_spout1",
                                    [stream1, stream2],
                                    spout_parallelism)
    topology.spouts.extend([spout1])

    # Bolts
    bolt1 = self.create_mock_bolt("mock_bolt1",
                                  [stream1],
                                  [stream3],
                                  bolt1_parallelism)
    bolt2 = self.create_mock_bolt("mock_bolt2",
                                  [stream2],
                                  [stream4],
                                  bolt2_parallelism)
    bolt3 = self.create_mock_bolt("mock_bolt3",
                                  [stream3, stream4],
                                  [],
                                  bolt3_parallelism)
    topology.bolts.extend([bolt1, bolt2, bolt3])


    return topology

  def create_mock_simple_physical_plan(
      self,
      spout_parallelism=1,
      bolt_parallelism=1):
    pplan = protoPPlan.PhysicalPlan()
    pplan.topology.CopyFrom(self.create_mock_simple_topology(
        spout_parallelism,
        bolt_parallelism))
    return pplan

  def create_mock_medium_physical_plan(
      self,
      spout_parallelism=1,
      bolt1_parallelism=1,
      bolt2_parallelism=1,
      bolt3_parallelism=1):
    pplan = protoPPlan.PhysicalPlan()
    pplan.topology.CopyFrom(self.create_mock_medium_topology(
        spout_parallelism,
        bolt1_parallelism,
        bolt2_parallelism,
        bolt3_parallelism))
    return pplan

  def create_mock_execution_state(self):
    estate = protoEState.ExecutionState()
    estate.topology_name = MockProto.topology_name
    estate.topology_id = MockProto.topology_id
    estate.cluster = MockProto.cluster
    estate.environ = MockProto.environ
    return estate

  def create_mock_tmaster(self):
    tmaster = protoTmaster.TMasterLocation()
    return tmaster

  def add_topology_config(self, topology, key, value):
    kv = topology.topology_config.kvs.add()
    kv.key = key
    kv.type = protoTopology.ConfigValueType.Value('STRING_VALUE')
    kv.value = str(value)
