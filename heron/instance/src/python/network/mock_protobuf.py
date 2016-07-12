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

def get_mock_config():
  return topology_pb2.Config()


def get_mock_component(name="component_name",
                       config=get_mock_config(),
                       python_cls="heron.instance.src.python.example.word_spout.WordSpout"):
  component = topology_pb2.Component()
  component.name = name
  component.python_class_name = python_cls
  component.config.CopyFrom(config)
  return component

def get_mock_bolt(component=get_mock_component(), inputs=[], outputs=[]):
  bolt = topology_pb2.Bolt()
  bolt.comp.CopyFrom(component)

  for i in inputs:
    added = bolt.inputs.add()
    added.CopyFrom(i)

  for o in outputs:
    added = bolt.outputs.add()
    added.CopyFrom(o)

  return bolt

def get_mock_spout(component=get_mock_component(), outputs=[]):
  spout = topology_pb2.Spout()
  spout.comp.CopyFrom(component)

  for out in outputs:
    added = spout.outputs.add()
    added.CopyFrom(out)

  return spout

def get_mock_topology(id="topology_id", name="topology_name", state=1, spouts=[], bolts=[]):
  # topology
  topology = topology_pb2.Topology()
  topology.id = id
  topology.name = name
  topology.state = state

  for sp in spouts:
    added = topology.spouts.add()
    added.CopyFrom(sp)

  for bl in bolts:
    added = topology.bolts.add()
    added.CopyFrom(bl)

  return topology

def get_mock_stmgr(id="Stmgr_id", host="localhost", port=9999, endpoint="hello"):
  stmgr = physical_plan_pb2.StMgr()
  stmgr.id = id
  stmgr.host_name = host
  stmgr.data_port = port
  stmgr.local_endpoint = endpoint
  return stmgr

def get_mock_instance_info(task_id=123, component_index=23, component_name="hello"):
  # instance info
  instance_info = physical_plan_pb2.InstanceInfo()
  instance_info.task_id = task_id
  instance_info.component_index = component_index
  instance_info.component_name = component_name
  return instance_info

def get_mock_instance(instance_id="instance_id",
                      stmgr_id="Stmgr_id",
                      info=get_mock_instance_info()):
  instance = physical_plan_pb2.Instance()
  instance.instance_id = instance_id
  instance.stmgr_id = stmgr_id
  instance.info.CopyFrom(info)
  return instance

def get_mock_pplan(topology=get_mock_topology(),
                   stmgrs=[],
                   instances=[]):
  pplan = physical_plan_pb2.PhysicalPlan()
  pplan.topology.MergeFrom(topology)

  if len(stmgrs) == 0:
    stmgrs.append(get_mock_stmgr())
  if len(instances) == 0:
    instances.append(get_mock_instance())

  for stmgr in stmgrs:
    added = pplan.stmgrs.add()
    added.CopyFrom(stmgr)

  for instance in instances:
    added = pplan.instances.add()
    added.CopyFrom(instance)

  return pplan

def get_mock_status(status="OK", message="OKOKOK"):
  mock_status = common_pb2.Status()
  mock_status.status = common_pb2.StatusCode.Value(status)
  mock_status.message = message
  return mock_status

def get_mock_assignment_message(pplan=get_mock_pplan()):
  # message
  mock_message = stmgr_pb2.NewInstanceAssignmentMessage()
  mock_message.pplan.MergeFrom(pplan)
  return mock_message

def get_mock_register_response(status=get_mock_status(), pplan=get_mock_pplan()):
  mock_response = stmgr_pb2.RegisterInstanceResponse()
  mock_response.status.MergeFrom(status)
  mock_response.pplan.MergeFrom(pplan)
  return mock_response


#####

def get_pplan_builder_and_typename():
  builder = lambda : physical_plan_pb2.PhysicalPlan()
  typename = builder().DESCRIPTOR.full_name
  return builder, typename

def get_many_mock_pplans():
  pplans_lst = []
  for i in range(10):
    _id = "Stmgr-" + str(i)
    pplan = get_mock_pplan(stmgrs=[get_mock_stmgr(id=_id)])
    pplans_lst.append(pplan)
  return pplans_lst

