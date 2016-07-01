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

# Abstract class for Bolt/Spout -- Python interface of IInstance.java

from heron.common.src.python.color import Log
from heron.proto import tuple_pb2

from instance.serializer import PythonSerializer
from .outgoing_tuple_helper import OutgoingTupleHelper

# TODO: maybe implement some basic stuff


class BaseInstance(object):
  """The base class for heron bolt/spout instance

  Implements the following functionality:
  1. Basic output collector API and pushing tuples to Out-Stream
  2. Run tasks continually

  :ivar pplan_helper: Physical Plan Helper for this component
  :ivar in_stream:    In-Stream Heron Communicator
  :ivar output_helper: Outgoing Tuple Helper
  """

  DEFAULT_STREAM_ID = "default"
  make_data_tuple = lambda : tuple_pb2.HeronDataTuple()

  def __init__(self, pplan_helper, in_stream, out_stream, serializer=PythonSerializer()):
    self.pplan_helper = pplan_helper
    self.in_stream = in_stream
    self.serializer = serializer
    self.output_helper = OutgoingTupleHelper(self.pplan_helper, out_stream)

  def run_tasks(self):
    while True:
      self._run()

  def _serialize_obj(self, object):
    """Serialize object using the serializer for this instance

    :returns: Serialized bytestring
    """
    return str(self.serializer.serialize(object))

  def _admit_data_tuple(self, output_tuple, stream_id, is_spout, anchors=None, message_id=None):
    """Internal implementation of OutputCollector

    Handles emitting data tuples
    """
    # TODO (Important) : check whether this tuple is sane with pplan_helper.check_output_schema
    # TODO : custom grouping and invoke hook emit

    data_tuple = self.make_data_tuple()
    data_tuple.key = 0

    # TODO : set the anchors for a tuple (for Bolt), or message id (for Spout)

    tuple_size_in_bytes = 0

    # Serialize
    for object in output_tuple:
      serialized = self._serialize_obj(object)
      value = data_tuple.values.add()
      value = serialized
      tuple_size_in_bytes += len(serialized)

    self.output_helper.add_data_tuple(stream_id, data_tuple, tuple_size_in_bytes)

  ##################################################################
  # The followings are to be implemented by Spout/Bolt independently
  ##################################################################

  def start(self):
    """Do the basic setup for Heron Instance"""
    raise NotImplementedError

  def stop(self):
    """Do the basic clean for Heron Instance

    Note that this method is not guaranteed to be invoked
    """
    # TODO: We never actually call this method
    raise NotImplementedError

  def emit(self, output_tuple, stream_id=DEFAULT_STREAM_ID, anchors=None, message_id=None):
    """Emits a tuple from this instance"""
    raise NotImplementedError

  def _run(self):
    """Tasks to be executed every time this is waken up

    This is called inside of ``run_tasks()``.
    Equivalent to addSpoutTasks()/addBoltTasks() in Java implementation.
    Separated out so it can be properly unit tested.
    """
    raise NotImplementedError

  def _read_tuples_and_execute(self):
    """Read tuples from a queue and process the tuples"""
    raise NotImplementedError

  def _activate(self):
    """Activate the instance"""
    raise NotImplementedError

  def _deactivate(self):
    """Deactivate the instance"""
    raise NotImplementedError

