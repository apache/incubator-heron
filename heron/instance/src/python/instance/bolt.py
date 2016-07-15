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
from heron.instance.src.python.instance.comp_spec import HeronComponentSpec
from heron.instance.src.python.instance.tuple import TupleHelper

# TODO: declare output fields
class Bolt(BaseInstance):
  """The base class for all heron bolts in Python"""

  def __init__(self, pplan_helper, in_stream, out_stream):
    super(Bolt, self).__init__(pplan_helper, in_stream, out_stream)
    # TODO: bolt metrics

    if self.pplan_helper.is_spout:
      raise RuntimeError("No bolt in physical plan")

    # TODO: Topology context, serializer and sys config

  @classmethod
  def spec(cls, name=None, inputs=None, par=1, config=None):
    """Register this bolt to the topology abd create ``HeronComponentSpec``

    The usage of this method is compatible with StreamParse API, although it does not create
    ``ShellBoltSpec`` but instead directly registers to a ``Topology`` class.

    :type name: str
    :param name: Name of this bolt.
    :param inputs: Streams that feed into this Bolt.

                   Two forms of this are acceptable:

                   1. A `dict` mapping from ``HeronComponentSpec`` to ``Grouping``
                   2. A `list` of ``HeronComponentSpec`` or ``Stream``
    :type par: int
    :param par: Parallelism hint for this spout.
    :type config: dict
    :param config: Component-specific config settings.
    """
    python_class_path = cls.get_python_class_path()

    if hasattr(cls, 'outputs'):
      _outputs = cls.outputs
    else:
      _outputs = None

    return HeronComponentSpec(name, python_class_path, is_spout=False, par=par,
                              inputs=inputs, outputs=_outputs, config=config)

  def start(self):
    self.initialize()

  def stop(self):
    pass

  def emit(self, tup, stream=BaseInstance.DEFAULT_STREAM_ID, anchors=None, direct_task=None, need_task_ids=False):
    """Emits a new tuple from this Bolt

    It is compatible with StreamParse API.

    :type tup: list or tuple
    :param tup: the new output Tuple to send from this bolt, should only contain only serializable data.
    :type stream: str
    :param stream: the ID of the stream to emit this Tuple to. Leave empty to emit to the default stream.
    :type anchors: list
    :param anchors: a list of HeronTuples to which the emitted Tuples should be anchored.
    :type direct_task: int
    :param direct_task: the task to send the Tupel to if performing a direct emit.
    :type need_task_ids: bool
    :param need_task_ids: indicate whether or not you would like the task IDs the Tuple was emitted.
    """
    # TODO: return when need_task_ids=True
    return super(Bolt, self)._admit_data_tuple(tup, stream_id=stream, is_spout=False,
                                               anchors=anchors, message_id=None)

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

    # create HeronTuple
    tup = TupleHelper.make_tuple(stream, data_tuple.key, values, roots=None)
    self.process(tup)

  def _activate(self):
    pass

  def _deactivate(self):
    pass



  ###################################
  # API: To be implemented by users
  ###################################

  @abstractmethod
  def initialize(self, config=None, context=None):
    """Called when a task for this component is initialized within a worker on the cluster

    It is compatible with StreamParse API. (Parameter name changed from ``storm_conf`` to ``config``)

    It provides the bolt with the environment in which the bolt executes. A good place to
    initialize connections to data sources.

    *Should be implemented by a subclass.*

    :type config: dict
    :param config: The Heron configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
    :type context: dict
    :param context: This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
    """
    pass

  @abstractmethod
  def process(self, tup):
    """Process a single tuple of input

    The Tuple object contains metadata on it about which component/stream/task it came from.
    To emit a tuple, call ``self.emit(tuple)``.

    **Must be implemented by a subclass.**

    You can emit a tuple from this bolt by using ``self.emit()`` method.

    :type tup: ``Tuple``
    """
    # TODO: documentation and Tuple implementation
    raise NotImplementedError()

  def ack(self, tup):
    """Indicate that processing of a Tuple has succeeded

    It is compatible with StreamParse API.

    *Should be implemented by a subclass.*
    """
    pass

  def fail(self, tup):
    """Indicate that processing of a Tuple has failed

    It is compatible with StreamParse API.

    *Should be implemented by a subclass.*
    """
    pass

  def cleanup(self):
    pass




