# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
''' mock_proto.py '''
from heronpy.api import api_constants
import heron.proto.execution_state_pb2 as protoEState
import heron.proto.physical_plan_pb2 as protoPPlan
import heron.proto.packing_plan_pb2 as protoPackingPlan
import heron.proto.tmanager_pb2 as protoTmanager
import heron.proto.topology_pb2 as protoTopology

# pylint: disable=no-self-use, missing-docstring
class MockProto:
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
    kv.key = api_constants.TOPOLOGY_COMPONENT_PARALLELISM
    kv.type = protoTopology.ConfigValueType.Value('STRING_VALUE')
    kv.value = str(spout_parallelism)
    for stream in output_streams:
      spout.outputs.add().stream.CopyFrom(stream)
    return spout

  def create_mock_resource(self):
    resource = protoPackingPlan.Resource()
    resource.cpu = 1.0
    resource.ram = 1024
    resource.disk = 1024 * 2
    return resource

  def create_mock_instance_plan(self):
    instancePlan = protoPackingPlan.InstancePlan()
    instancePlan.component_name  = "word"
    instancePlan.task_id = 1
    instancePlan.component_index = 1
    instancePlan.resource.CopyFrom(self.create_mock_resource())
    return instancePlan

  def create_mock_simple_container_plan(self):
    containerPlan = protoPackingPlan.ContainerPlan()
    containerPlan.id = 1
    containerPlan.instance_plans.extend([self.create_mock_instance_plan()])
    containerPlan.requiredResource.CopyFrom(self.create_mock_resource())

    return containerPlan

  def create_mock_simple_container_plan2(self):
    containerPlan = protoPackingPlan.ContainerPlan()
    containerPlan.id = 1
    containerPlan.instance_plans.extend([self.create_mock_instance_plan()])
    containerPlan.requiredResource.CopyFrom(self.create_mock_resource())
    containerPlan.scheduledResource.CopyFrom(self.create_mock_resource())
    return containerPlan

  def create_mock_bolt(self,
                       bolt_name,
                       input_streams,
                       output_streams,
                       bolt_parallelism):
    bolt = protoTopology.Bolt()
    bolt.comp.name = bolt_name
    kv = bolt.comp.config.kvs.add()
    kv.key = api_constants.TOPOLOGY_COMPONENT_PARALLELISM
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

  def create_mock_stream_manager(self):
    stmgr = protoPPlan.StMgr()
    stmgr.id = "mock_stream1"
    stmgr.host_name = "local"
    stmgr.local_endpoint = ":1000"
    stmgr.local_data_port = 1001
    stmgr.shell_port = 1002
    stmgr.data_port = 1003
    stmgr.cwd = ""
    stmgr.pid = 1
    return stmgr

  def create_mock_instance(self, i):
    instance = protoPPlan.Instance()
    instance.instance_id = f"mock_instance{i}"
    instance.stmgr_id = f"mock_stream{i}"
    instance_info = protoPPlan.InstanceInfo()
    instance_info.task_id = i
    instance_info.component_index = i
    instance_info.component_name = f"mock_spout"
    instance.info.CopyFrom(instance_info)
    return instance

  def create_mock_simple_physical_plan(
      self,
      spout_parallelism=1,
      bolt_parallelism=1,
      instances_num=1):
    pplan = protoPPlan.PhysicalPlan()
    pplan.topology.CopyFrom(self.create_mock_simple_topology(
        spout_parallelism,
        bolt_parallelism))
    pplan.stmgrs.extend([self.create_mock_stream_manager()])
    instances = []
    for i in range(instances_num):
      instances.append(self.create_mock_instance(i+1))
    pplan.instances.extend(instances)
    return pplan

  def create_mock_simple_packing_plan(
    self):
    packingPlan = protoPackingPlan.PackingPlan()
    packingPlan.id = "ExclamationTopology"
    packingPlan.container_plans.extend([self.create_mock_simple_container_plan()])
    return packingPlan

  def create_mock_simple_packing_plan2(
    self):
    packingPlan = protoPackingPlan.PackingPlan()
    packingPlan.id = "ExclamationTopology"
    packingPlan.container_plans.extend([self.create_mock_simple_container_plan2()])
    packingPlan.container_plans.extend([self.create_mock_simple_container_plan()])
    return packingPlan

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

  def create_mock_tmanager(self):
    tmanager = protoTmanager.TManagerLocation()
    return tmanager

  def add_topology_config(self, topology, key, value):
    kv = topology.topology_config.kvs.add()
    kv.key = key
    kv.type = protoTopology.ConfigValueType.Value('STRING_VALUE')
    kv.value = str(value)
