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
"""mock_generator.py: module for creating mock objects for unittesting
                      mainly for common/tests/python/utils"""

from heron.common.src.python.utils.metrics import MetricsCollector
from heron.common.src.python.utils.misc import (OutgoingTupleHelper, PhysicalPlanHelper,
                                                HeronCommunicator, PythonSerializer)
from heron.proto import tuple_pb2

import heron.common.src.python.constants as constants
import heron.common.tests.python.mock_protobuf as mock_protobuf

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
  zipped = zip(instance_ids, task_ids, comp_indexes, comp_names)
  return pplan, [dict(zip(keys, z)) for z in zipped]

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
      super(MockOutgoingTupleHelper, self).__init__(pplan_helper, out_stream, sample_sys_config)

  @staticmethod
  def _prepare_sample_success():
    pplan, instances = get_a_sample_pplan()
    pplan_helper = PhysicalPlanHelper(pplan, instances[0]["instance_id"])
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
