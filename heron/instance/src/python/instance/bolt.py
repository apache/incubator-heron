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

import Queue
from abc import abstractmethod

from .base_instance import BaseInstance
from heron.proto import tuple_pb2
from heron.common.src.python.log import Log

# TODO: declare output fields
class Bolt(BaseInstance):
  """The base class for all heron bolts in Python"""
  def __init__(self, pplan_helper, in_stream, out_stream):
    super(Bolt, self).__init__(pplan_helper, in_stream, out_stream)
    # TODO: bolt metrics

    if self.pplan_helper.is_spout:
      raise RuntimeError("No bolt in physical plan")

    # TODO: Topology context, serializer and sys config

  def start(self):
    self.prepare(None, None)

  def stop(self):
    pass

  def emit(self, output_tuple, stream_id=BaseInstance.DEFAULT_STREAM_ID, anchors=None, message_id=None):
    """Emits a new tuple from this Bolt

    :type output_tuple: list
    :param output_tuple: The new output tuple from this bolt as a list of serializable objects
    :type stream_id: str
    :param stream_id: The stream to emit to
    :type anchors: list
    :param anchors: The tuples to anchor to
    :param message_id: Must be ``None`` for Bolt
    """
    if message_id is not None:
      raise RuntimeError("message_id cannot be set for emit() from Bolt")

    self._admit_data_tuple(output_tuple, stream_id, is_spout=False, anchors=anchors, message_id=None)

  def _run(self):
    self._read_tuples_and_execute()
    self.output_helper.send_out_tuples()

  def run_in_single_thread(self):
    self._run()

  def _read_tuples_and_execute(self):
    while not self.in_stream.is_empty():
      try:
        tuples = self.in_stream.poll()
      except Queue.Empty:
        break

      # TODO: Topology Context

      if isinstance(tuples, tuple_pb2.HeronTupleSet):
        if tuples.HasField("control"):
          raise RuntimeError("Bolt cannot get acks/fails from other components")
        elif tuples.HasField("data"):
          stream = tuples.data.stream

          for data_tuple in tuples.data.tuples:
            self._handle_data_tuple(data_tuple, stream)
        else:
          Log.error("Received tuple neither data nor control")
      else:
        Log.error("Received tuple not instance of HeronTupleSet")

  def _handle_data_tuple(self, data_tuple, stream):
    values = []
    for value in data_tuple.values:
      values.append(self.serializer.deserialize(value))

    self.execute(values)

  def _activate(self):
    pass

  def _deactivate(self):
    pass



  ###################################
  # API: To be implemented by users
  ###################################

  @abstractmethod
  def prepare(self, heron_config, context):
    """Called when a task for this component is initialized within a worker on the cluster

    It provides the bolt with the environment in which the bolt executes.

    *Must be implemented by a subclass.*

    :param heron_config: The Heron configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
    :param context: This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
    """
    pass

  @abstractmethod
  def execute(self, tuple):
    """Process a single tuple of input

    The Tuple object contains metadata on it about which component/stream/task it came from.
    To emit a tuple, call ``self.emit(tuple)``.

    *Must be implemented by a subclass.*

    You can emit a tuple from this bolt by using ``self.emit()`` method.

    :type tuple: list
    """
    # TODO: documentation and Tuple implementation
    pass

  def cleanup(self):
    pass



