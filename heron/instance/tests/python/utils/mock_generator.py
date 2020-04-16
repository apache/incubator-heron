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

"""mock_generator.py: module for creating mock objects for unittesting
                      mainly for common/tests/python/utils"""

# pylint: disable=unused-argument
# pylint: disable=missing-docstring
import random
from mock import patch

from heronpy.api.task_hook import ITaskHook
from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.api.serializer import PythonSerializer

from heron.instance.src.python.utils.metrics import MetricsCollector
from heron.instance.src.python.utils.misc import (OutgoingTupleHelper, PhysicalPlanHelper,
                                                HeronCommunicator)
from heron.proto import tuple_pb2

import heron.instance.src.python.utils.system_constants as constants
import heron.instance.tests.python.mock_protobuf as mock_protobuf

prim_list = [1000, -234, 0.00023, "string",
             ["abc", "def", "ghi"], True, False,
             ("tuple", 123, True), None, {}]

# Returns legit looking topology
def get_a_sample_pplan():
  """Returns a legitimate looking physical plan

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

  :returns: PhysicalPlan message and a list of dictionaries for each instance containing
            (instance_id, task_id, comp_index, comp_name)
  """

  spout_1 = mock_protobuf.get_mock_spout(component=mock_protobuf.get_mock_component(name="spout1"))
  bolt_1 = mock_protobuf.get_mock_bolt(component=mock_protobuf.get_mock_component(name="bolt1"))
  bolt_2 = mock_protobuf.get_mock_bolt(component=mock_protobuf.get_mock_component(name="bolt2"))

  topology = mock_protobuf.get_mock_topology(spouts=[spout_1], bolts=[bolt_1, bolt_2])


  instance_ids = ["instance1", "instance2", "instancer3"]
  task_ids = [100, 200, 300]
  comp_indexes = [0, 0, 0]
  comp_names = ["spout1", "bolt1", "bolt2"]
  instances = []

  for i_id, t_id, c_i, c_name in zip(instance_ids, task_ids, comp_indexes, comp_names):
    info = mock_protobuf.get_mock_instance_info(task_id=t_id,
                                                component_index=c_i,
                                                component_name=c_name)
    instance = mock_protobuf.get_mock_instance(instance_id=i_id, info=info)
    instances.append(instance)

  pplan = mock_protobuf.get_mock_pplan(topology=topology, instances=instances)

  keys = ["instance_id", "task_id", "comp_index", "comp_name"]
  zipped = list(zip(instance_ids, task_ids, comp_indexes, comp_names))
  return pplan, [dict(list(zip(keys, z))) for z in zipped]

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

class MockOutgoingTupleHelper(OutgoingTupleHelper):
  """Creates a mock OutgoingTupleHelper class, for unittesting"""
  SAMPLE_SUCCESS = 0
  def __init__(self, mode=SAMPLE_SUCCESS):
    self.called_init_new_data = False
    self.called_init_new_control = False
    sample_sys_config = {constants.INSTANCE_SET_DATA_TUPLE_CAPACITY: 1000,
                         constants.INSTANCE_SET_DATA_TUPLE_SIZE_BYTES: 2000,
                         constants.INSTANCE_SET_CONTROL_TUPLE_CAPACITY: 3000}

    if mode == MockOutgoingTupleHelper.SAMPLE_SUCCESS:
      pplan_helper, out_stream = self._prepare_sample_success()
      with patch("heron.instance.src.python.utils.system_config.get_sys_config",
                 side_effect=lambda: sample_sys_config):
        super(MockOutgoingTupleHelper, self).__init__(pplan_helper, out_stream)

  @staticmethod
  def _prepare_sample_success():
    pplan, instances = get_a_sample_pplan()
    pplan_helper = PhysicalPlanHelper(pplan, instances[0]["instance_id"], "topology.pex.path")
    out_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)
    return pplan_helper, out_stream

  def _init_new_data_tuple(self, stream_id):
    self.called_init_new_data = True
    OutgoingTupleHelper._init_new_data_tuple(self, stream_id)

  def _init_new_control_tuple(self):
    self.called_init_new_control = True
    OutgoingTupleHelper._init_new_control_tuple(self)

class MockMetricsCollector(MetricsCollector):
  """Creates a mock MetricsCollector class, for unittesting"""
  def __init__(self):
    self.registered_timers = []
    super(MockMetricsCollector, self).__init__(None, HeronCommunicator())

  def _register_timer_task(self, time_bucket_in_sec):
    self.registered_timers.append(time_bucket_in_sec)

class MockCustomGrouping(ICustomGrouping):
  ALL_TARGET_MODE = 0   # returns the whole list of target tasks
  RANDOM_TARGET_MODE = 1
  WRONG_RETURN_TYPE_MODE = 2
  WRONG_CHOOSE_TASK_MODE = 3

  def __init__(self, mode):
    super(MockCustomGrouping, self).__init__()
    self.mode = mode

  def prepare(self, context, component, stream, target_tasks):
    self.target_tasks = target_tasks

  def choose_tasks(self, values):
    if self.mode == self.ALL_TARGET_MODE:
      return self.target_tasks
    elif self.mode == self.RANDOM_TARGET_MODE:
      return [task for task in self.target_tasks if bool(random.getrandbits(1))]
    elif self.mode == self.WRONG_RETURN_TYPE_MODE:
      return 'string'
    elif self.mode == self.WRONG_CHOOSE_TASK_MODE:
      ret = []
      while len(ret) < 5:
        i = random.randint(1, 1000)
        if i not in self.target_tasks and i not in ret:
          ret.append(i)
        else:
          continue
      return ret

class MockTaskHook(ITaskHook):
  def prepare(self, conf, context):
    self.clean_up_called = False
    self.emit_called = False
    self.spout_ack_called = False
    self.spout_fail_called = False
    self.bolt_exec_called = False
    self.bolt_ack_called = False
    self.bolt_fail_called = False

  def emit(self, emit_info):
    self.emit_called = True

  def spout_ack(self, spout_ack_info):
    self.spout_ack_called = True

  def spout_fail(self, spout_fail_info):
    self.spout_fail_called = True

  def bolt_execute(self, bolt_execute_info):
    self.bolt_exec_called = True

  def bolt_ack(self, bolt_ack_info):
    self.bolt_ack_called = True

  def bolt_fail(self, bolt_fail_info):
    self.bolt_fail_called = True
