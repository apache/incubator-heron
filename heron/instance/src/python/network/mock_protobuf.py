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
from heron.proto import stmgr_pb2, physical_plan_pb2, topology_pb2, common_pb2


def get_mock_topology():
  # topology
  topology = topology_pb2.Topology()
  topology.id = "topology_id"
  topology.name = "topology_name"
  topology.state = 1
  return topology

def get_mock_stmgr():
  stmgr = physical_plan_pb2.StMgr()
  stmgr.id = "Stmgr_id"
  stmgr.host_name = "localhost"
  stmgr.data_port = 9999
  stmgr.local_endpoint = "hello"
  return stmgr

def get_mock_instance():
  # instance info
  instance_info = physical_plan_pb2.InstanceInfo()
  instance_info.task_id = 123
  instance_info.component_index = 23
  instance_info.component_name = "hello"

  instance = physical_plan_pb2.Instance()
  instance.instance_id = "instance_id_is_this"
  instance.stmgr_id = "stmgr_id_is_this"
  instance.info.MergeFrom(instance_info)

  return instance

def get_mock_pplan():
  pplan = physical_plan_pb2.PhysicalPlan()
  pplan.topology.MergeFrom(get_mock_topology())

  sample_stmgr = pplan.stmgrs.add()
  sample_stmgr.CopyFrom(get_mock_stmgr())

  sample_instance = pplan.instances.add()
  sample_instance.CopyFrom(get_mock_instance())

  return pplan

def get_mock_status():
  mock_status = common_pb2.Status()
  mock_status.status = 1
  mock_status.message = "OKOKOK"
  return mock_status

def get_mock_assignment_message():
  # message
  mock_message = stmgr_pb2.NewInstanceAssignmentMessage()
  mock_message.pplan.MergeFrom(get_mock_pplan())

  return mock_message

def get_mock_register_response():
  mock_response = stmgr_pb2.RegisterInstanceResponse()
  mock_response.status.MergeFrom(get_mock_status())
  mock_response.pplan.MergeFrom(get_mock_pplan())
  return mock_response

