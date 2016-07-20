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

import time

from .component import Component
from heron.proto import tuple_pb2
from heron.common.src.python.log import Log
from heron.instance.src.python.instance.comp_spec import HeronComponentSpec
from heron.instance.src.python.instance.tuple import TupleHelper
from heron.instance.src.python.metrics.metrics_helper import BoltMetrics

import heron.common.src.python.constants as constants

# TODO: declare output fields
class Bolt(Component):
  """The base class for all heron bolts in Python"""

  def __init__(self, pplan_helper, in_stream, out_stream, looper, sys_config):
    super(Bolt, self).__init__(pplan_helper, in_stream, out_stream, looper, sys_config)
    # TODO: bolt metrics

    if self.pplan_helper.is_spout:
      raise RuntimeError("No bolt in physical plan")

    self.bolt_config = self.pplan_helper.context['config']

    self.bolt_metrics = BoltMetrics(self.pplan_helper)

    # TODO: Topology context, serializer and sys config

  @classmethod
  def spec(cls, name=None, inputs=None, par=1, config=None):
    """Register this bolt to the topology and create ``HeronComponentSpec``

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
    self.bolt_metrics.register_metrics(self.pplan_helper.context, self.sys_config)
    self.initialize(config=self.bolt_config, context=self.pplan_helper.context)

    # prepare tick tuple
    self._prepare_tick_tup_timer()

  def stop(self):
    pass

  def emit(self, tup, stream=Component.DEFAULT_STREAM_ID, anchors=None, direct_task=None, need_task_ids=False):
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
    # TODO: check whether this tuple is sane with pplan_helper.check_output_schema
    # TODO: custom grouping and invoke hook emit
    data_tuple = tuple_pb2.HeronDataTuple()
    data_tuple.key = 0

    tuple_size_in_bytes = 0

    # Serialize
    for obj in tup:
      serialized = self.serializer.serialize(obj)
      data_tuple.values.append(serialized)
      tuple_size_in_bytes += len(serialized)

    # TODO: return when need_task_ids=True
    ret = super(Bolt, self).admit_data_tuple(stream_id=stream, data_tuple=data_tuple,
                                             tuple_size_in_bytes=tuple_size_in_bytes)

    self.bolt_metrics.update_emit_count(stream)
    return ret

  @staticmethod
  def is_tick(tup):
    """Returns whether or not the given HeronTuple is a tick Tuple

    It is compatible with StreamParse API.
    """
    return tup.stream == TupleHelper.TICK_TUPLE_ID

  def process_incoming_tuples(self):
    """Should be called when tuple was buffered into in_stream"""
    self._read_tuples_and_execute()
    self.output_helper.send_out_tuples()

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
    start_time = time.time()

    values = []
    for value in data_tuple.values:
      values.append(self.serializer.deserialize(value))

    # create HeronTuple
    tup = TupleHelper.make_tuple(stream, data_tuple.key, values, roots=None)

    deserialized_time = time.time()
    self.process(tup)
    execute_latency = time.time() - deserialized_time

    self.bolt_metrics.execute_tuple(stream.id, stream.component_name,
                                    execute_latency * constants.SEC_TO_NS)


  def _activate(self):
    pass

  def _deactivate(self):
    pass

  def _prepare_tick_tup_timer(self):
    if constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS in self.bolt_config:
      tick_freq_sec = self.bolt_config[constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS]
      Log.debug("Tick Tuple Frequency: " + tick_freq_sec + " sec.")

      def send_tick():
        tick = TupleHelper.make_tick_tuple()
        start_time = time.time()
        self.process(tick)
        latency = time.time() - start_time
        self.bolt_metrics.execute_tuple(tick.id, tick.component,
                                        latency * constants.SEC_TO_NS)
        self.output_helper.send_out_tuples()
        self.looper.wake_up() # so emitted tuples would be added to buffer now
        self._prepare_tick_tup_timer()

      self.looper.register_timer_task_in_sec(send_tick, tick_freq_sec)



  ###################################
  # API: To be implemented by users
  ###################################

  @abstractmethod
  def initialize(self, config={}, context={}):
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





