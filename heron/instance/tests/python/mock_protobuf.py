#!/usr/bin/env python
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

"""mock_protobuf.py: provides methods to create dummy protobuf message for testing

This module is used mainly for testing Python Heron Instance as well as common modules
written in Python.
"""

from heron.proto import stmgr_pb2, physical_plan_pb2, topology_pb2, common_pb2
from heronpy.api.serializer import PythonSerializer

# pylint: disable=dangerous-default-value
# pylint: disable=redefined-builtin

def get_mock_config(config_dict=None):
  """Returns a protobuf Config object from topology_pb2"""
  if config_dict is None:
    return topology_pb2.Config()

  proto_config = topology_pb2.Config()
  config_serializer = PythonSerializer()
  assert isinstance(config_dict, dict)
  for key, value in list(config_dict.items()):
    if isinstance(value, bool):
      kvs = proto_config.kvs.add()
      kvs.key = key
      kvs.value = "true" if value else "false"
      kvs.type = topology_pb2.ConfigValueType.Value("STRING_VALUE")
    elif isinstance(value, (str, int, float)):
      kvs = proto_config.kvs.add()
      kvs.key = key
      kvs.value = str(value)
      kvs.type = topology_pb2.ConfigValueType.Value("STRING_VALUE")
    else:
      kvs = proto_config.kvs.add()
      kvs.key = key
      kvs.serialized_value = config_serializer.serialize(value)
      kvs.type = topology_pb2.ConfigValueType.Value("PYTHON_SERIALIZED_VALUE")

  return proto_config

def get_mock_component(name="component_name",
                       config=get_mock_config(),
                       python_cls="heron.instance.src.python.example.word_spout.WordSpout"):
  """Returns a mock protobuf Component object from topology_pb2"""
  component = topology_pb2.Component()
  component.name = name
  component.spec = topology_pb2.ComponentObjectSpec.Value("PYTHON_CLASS_NAME")
  component.class_name = python_cls
  component.config.CopyFrom(config)
  return component

def get_mock_stream_id(id="stream_id", component_name="component_name"):
  """Returns a mock protobuf StreamId from topology_pb2"""
  stream_id = topology_pb2.StreamId()
  stream_id.id = id
  stream_id.component_name = component_name
  return stream_id

def get_mock_bolt(component=get_mock_component(), inputs=[], outputs=[]):
  """Returns a mock protobuf Bolt object from topology_pb2"""
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
  """Returns a mock protobuf Spout object from topology_pb2"""
  spout = topology_pb2.Spout()
  spout.comp.CopyFrom(component)

  for out in outputs:
    added = spout.outputs.add()
    added.CopyFrom(out)

  return spout

def get_mock_topology(id="topology_id", name="topology_name", state=1, spouts=[], bolts=[]):
  """Returns a mock protobuf Topology object from topology_pb2"""
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

def get_mock_stmgr(id="Stmgr_id", host="127.0.0.1", port=9999, endpoint="hello"):
  """Returns a mock protobuf StMgr object from physical_plan_pb2"""
  stmgr = physical_plan_pb2.StMgr()
  stmgr.id = id
  stmgr.host_name = host
  stmgr.data_port = port
  stmgr.local_endpoint = endpoint
  return stmgr

def get_mock_instance_info(task_id=123, component_index=23, component_name="hello"):
  """Returns a mock protobuf InstanceInfo object from physical_plan_pb2"""
  # instance info
  instance_info = physical_plan_pb2.InstanceInfo()
  instance_info.task_id = task_id
  instance_info.component_index = component_index
  instance_info.component_name = component_name
  return instance_info

def get_mock_instance(instance_id="instance_id",
                      stmgr_id="Stmgr_id",
                      info=get_mock_instance_info()):
  """Returns a mock protobuf Instance object from physical_plan_pb2"""
  instance = physical_plan_pb2.Instance()
  instance.instance_id = instance_id
  instance.stmgr_id = stmgr_id
  instance.info.CopyFrom(info)
  return instance

def get_mock_pplan(topology=get_mock_topology(),
                   stmgrs=[],
                   instances=[]):
  """Returns a mock protobuf PhysicalPlan object from physical_plan_pb2"""
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

def get_mock_status(status="OK", message="OK Message"):
  """Returns a mock protobuf Status object from common_pb2"""
  mock_status = common_pb2.Status()
  mock_status.status = common_pb2.StatusCode.Value(status)
  mock_status.message = message
  return mock_status

def get_mock_assignment_message(pplan=get_mock_pplan()):
  """Returns a mock protobuf NewInstanceAssignmentMessage object from stmgr_pb2"""
  # message
  mock_message = stmgr_pb2.NewInstanceAssignmentMessage()
  mock_message.pplan.MergeFrom(pplan)
  return mock_message

def get_mock_register_response(status=get_mock_status(), pplan=get_mock_pplan()):
  """Returns a mock protobuf RegisterInstanceResponse object from stmgr_pb2"""
  mock_response = stmgr_pb2.RegisterInstanceResponse()
  mock_response.status.MergeFrom(status)
  mock_response.pplan.MergeFrom(pplan)
  return mock_response

#####

def get_pplan_builder_and_typename():
  """Returns a PhysicalPlan builder callable and typename 'PhysicalPlan'"""
  # pylint: disable=unnecessary-lambda
  builder = lambda: physical_plan_pb2.PhysicalPlan()
  typename = builder().DESCRIPTOR.full_name
  return builder, typename

def get_many_mock_pplans():
  """Returns a list of 10 PhysicalPlan objects, differing just by stream manager id"""
  pplans_lst = []
  for i in range(10):
    _id = "Stmgr-" + str(i)
    pplan = get_mock_pplan(stmgrs=[get_mock_stmgr(id=_id)])
    pplans_lst.append(pplan)
  return pplans_lst
