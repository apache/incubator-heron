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
from heron.common.src.python.utils.misc import PhysicalPlanHelper, PythonSerializer, HeronCommunicator, OutgoingTupleHelper
from heron.common.src.python.network import HeronClient, REQID, HeronProtocol, IncomingPacket, StatusCode

import heron.instance.src.python.network.mock_protobuf as mock_protobuf


#TODO: move most of the stuff to mock_generator in common





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
    stream = HeronCommunicator(producer_cb=None, consumer_cb=None)
    in_stream = stream
    out_stream = stream
    return pplan_helper, in_stream, out_stream

  def _handle_data_tuple(self, data_tuple, stream):
    self.received_data_tuple = data_tuple
    Bolt._handle_data_tuple(self, data_tuple, stream)
