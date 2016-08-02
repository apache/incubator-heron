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
'''bolt.py: module for base bolt for python topology'''

import time
import Queue

from abc import abstractmethod
from heron.proto import tuple_pb2
from heron.common.src.python.log import Log
from heron.common.src.python.utils.tuple import TupleHelper, HeronTuple
from heron.common.src.python.utils.metrics import BoltMetrics

import heron.common.src.python.constants as constants

from .component import Component, HeronComponentSpec

# pylint: disable=fixme
class Bolt(Component):
  """The base class for all heron bolts in Python"""

  def __init__(self, pplan_helper, in_stream, out_stream, looper, sys_config):
    super(Bolt, self).__init__(pplan_helper, in_stream, out_stream, looper, sys_config)

    if self.pplan_helper.is_spout:
      raise RuntimeError("No bolt in physical plan")

    # bolt_config is auto-typed, not <str -> str> only
    context = self.pplan_helper.context
    self.bolt_metrics = BoltMetrics(self.pplan_helper)

    # acking related
    self.acking_enabled = context.get_cluster_config().get(constants.TOPOLOGY_ENABLE_ACKING, False)
    Log.info("Enable ACK: %s" % str(self.acking_enabled))

  # pylint: disable=no-member
  @classmethod
  def spec(cls, name=None, inputs=None, par=1, config=None):
    """Register this bolt to the topology and create ``HeronComponentSpec``

    The usage of this method is compatible with StreamParse API, although it does not create
    ``ShellBoltSpec`` but instead directly registers to a ``Topology`` class.

    This method does not take a ``outputs`` argument because ``outputs`` should be
    an attribute of your ``Spout`` subclass. Also, some ways of declaring inputs is not supported
    in this implementation; please read the documentation below.

    :type name: str
    :param name: Name of this bolt.
    :param inputs: Streams that feed into this Bolt.

                   Two forms of this are acceptable:

                   1. A `dict` mapping from ``HeronComponentSpec`` to ``Grouping``.
                      In this case, default stream is used.
                   2. A `dict` mapping from ``GlobalStreamId`` to ``Grouping``.
                      This ``GlobalStreamId`` object itself is different from StreamParse, because
                      Heron does not use thrift, although its constructor method is compatible.
                   3. A `list` of ``HeronComponentSpec``. In this case, default stream with
                      SHUFFLE grouping is used.
                   4. A `list` of ``GlobalStreamId``. In this case, SHUFFLE grouping is used.
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
    context = self.pplan_helper.context
    self.bolt_metrics.register_metrics(context, self.sys_config)
    self.initialize(config=context.get_cluster_config(), context=context)
    context.invoke_hook_prepare()

    # prepare tick tuple
    self._prepare_tick_tup_timer()

  def stop(self):
    self.pplan_helper.context.invoke_hook_cleanup()
    self.cleanup()
    self.looper.exit_loop()

  def invoke_activate(self):
    pass

  def invoke_deactivate(self):
    pass

  # pylint: disable=unused-argument
  def emit(self, tup, stream=Component.DEFAULT_STREAM_ID,
           anchors=None, direct_task=None, need_task_ids=False):
    """Emits a new tuple from this Bolt

    It is compatible with StreamParse API.

    :type tup: list or tuple
    :param tup: the new output Tuple to send from this bolt,
                should only contain only serializable data.
    :type stream: str
    :param stream: the ID of the stream to emit this Tuple to.
                   Leave empty to emit to the default stream.
    :type anchors: list
    :param anchors: a list of HeronTuples to which the emitted Tuples should be anchored.
    :type direct_task: int
    :param direct_task: the task to send the Tupel to if performing a direct emit.
    :type need_task_ids: bool
    :param need_task_ids: indicate whether or not you would like the task IDs the Tuple was emitted.
    """
    # first check whether this tuple is sane
    self.pplan_helper.check_output_schema(stream, tup)

    # TODO: custom grouping
    self.pplan_helper.context.invoke_hook_emit(tup, stream, None)

    data_tuple = tuple_pb2.HeronDataTuple()
    data_tuple.key = 0

    # Set the anchors for a tuple
    if anchors is not None:
      merged_roots = set()
      for tup in [t for t in anchors if isinstance(t, HeronTuple) and t.roots is not None]:
        merged_roots.update(tup.roots)
      for rt in merged_roots:
        to_add = data_tuple.roots.add()
        to_add.CopyFrom(rt)

    tuple_size_in_bytes = 0
    start_time = time.time()

    # Serialize
    for obj in tup:
      serialized = self.serializer.serialize(obj)
      data_tuple.values.append(serialized)
      tuple_size_in_bytes += len(serialized)
    serialize_latency_ns = (time.time() - start_time) * constants.SEC_TO_NS
    self.bolt_metrics.serialize_data_tuple(stream, serialize_latency_ns)

    # TODO: return when need_task_ids=True
    ret = super(Bolt, self).admit_data_tuple(stream_id=stream, data_tuple=data_tuple,
                                             tuple_size_in_bytes=tuple_size_in_bytes)

    self.bolt_metrics.update_emit_count(stream)
    return ret

  def process_incoming_tuples(self):
    """Should be called when tuple was buffered into in_stream"""
    self._read_tuples_and_execute()
    self.output_helper.send_out_tuples()

  def _read_tuples_and_execute(self):
    start_cycle_time = time.time()
    total_data_emitted_bytes_before = self.get_total_data_emitted_in_bytes()
    exec_batch_time = \
      self.sys_config[constants.INSTANCE_EXECUTE_BATCH_TIME_MS] * constants.MS_TO_SEC
    exec_batch_size = self.sys_config[constants.INSTANCE_EXECUTE_BATCH_SIZE_BYTES]
    while not self.in_stream.is_empty():
      try:
        tuples = self.in_stream.poll()
      except Queue.Empty:
        break

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

      if (time.time() - start_cycle_time - exec_batch_time > 0) or \
          (self.get_total_data_emitted_in_bytes() - total_data_emitted_bytes_before
           > exec_batch_size):
        # batch reached
        break

  def _handle_data_tuple(self, data_tuple, stream):
    start_time = time.time()

    values = []
    for value in data_tuple.values:
      values.append(self.serializer.deserialize(value))

    # create HeronTuple
    tup = TupleHelper.make_tuple(stream, data_tuple.key, values, roots=data_tuple.roots)

    deserialized_time = time.time()
    self.process(tup)
    execute_latency_ns = (time.time() - deserialized_time) * constants.SEC_TO_NS
    deserialize_latency_ns = (deserialized_time - start_time) * constants.SEC_TO_NS

    self.pplan_helper.context.invoke_hook_bolt_execute(tup, execute_latency_ns)

    self.bolt_metrics.deserialize_data_tuple(stream.id, stream.component_name,
                                             deserialize_latency_ns)
    self.bolt_metrics.execute_tuple(stream.id, stream.component_name, execute_latency_ns)

  @staticmethod
  def is_tick(tup):
    """Returns whether or not the given HeronTuple is a tick Tuple

    It is compatible with StreamParse API.
    """
    return tup.stream == TupleHelper.TICK_TUPLE_ID

  def _prepare_tick_tup_timer(self):
    cluster_config = self.pplan_helper.context.get_cluster_config()
    if constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS in cluster_config:
      tick_freq_sec = cluster_config[constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS]
      Log.debug("Tick Tuple Frequency: %s sec." % str(tick_freq_sec))

      def send_tick():
        tick = TupleHelper.make_tick_tuple()
        start_time = time.time()
        self.process(tick)
        tick_execute_latency_ns = (time.time() - start_time) * constants.SEC_TO_NS
        self.bolt_metrics.execute_tuple(tick.id, tick.component, tick_execute_latency_ns)
        self.output_helper.send_out_tuples()
        self.looper.wake_up() # so emitted tuples would be added to buffer now
        self._prepare_tick_tup_timer()

      self.looper.register_timer_task_in_sec(send_tick, tick_freq_sec)


  def ack(self, tup):
    """Indicate that processing of a Tuple has succeeded

    It is compatible with StreamParse API.
    """
    if not isinstance(tup, HeronTuple):
      Log.error("Only HeronTuple type is supported in ack()")
      return

    if self.acking_enabled:
      ack_tuple = tuple_pb2.AckTuple()
      ack_tuple.ackedtuple = int(tup.id)

      tuple_size_in_bytes = 0
      for rt in tup.roots:
        to_add = ack_tuple.roots.add()
        to_add.CopyFrom(rt)
        tuple_size_in_bytes += rt.ByteSize()
      super(Bolt, self).admit_control_tuple(ack_tuple, tuple_size_in_bytes, True)

    process_latency_ns = (time.time() - tup.creation_time) * constants.SEC_TO_NS
    self.pplan_helper.context.invoke_hook_bolt_ack(tup, process_latency_ns)
    self.bolt_metrics.acked_tuple(tup.stream, tup.component, process_latency_ns)

  def fail(self, tup):
    """Indicate that processing of a Tuple has failed

    It is compatible with StreamParse API.
    """
    if not isinstance(tup, HeronTuple):
      Log.error("Only HeronTuple type is supported in fail()")
      return

    if self.acking_enabled:
      fail_tuple = tuple_pb2.AckTuple()
      fail_tuple.ackedtuple = int(tup.id)

      tuple_size_in_bytes = 0
      for rt in tup.roots:
        to_add = fail_tuple.roots.add()
        to_add.CopyFrom(rt)
        tuple_size_in_bytes += rt.ByteSize()
      super(Bolt, self).admit_control_tuple(fail_tuple, tuple_size_in_bytes, False)

    fail_latency_ns = (time.time() - tup.creation_time) * constants.SEC_TO_NS
    self.pplan_helper.context.invoke_hook_bolt_fail(tup, fail_latency_ns)
    self.bolt_metrics.failed_tuple(tup.stream, tup.component, fail_latency_ns)

  ###################################
  # API: To be implemented by users #
  ###################################

  @abstractmethod
  def initialize(self, config, context):
    """Called when a task for this component is initialized within a worker on the cluster

    It is compatible with StreamParse API.
    (Parameter name changed from ``storm_conf`` to ``config``)

    It provides the bolt with the environment in which the bolt executes. Note that
    you should NOT override ``__init__()`` for initialization of your bolt, as it is
    used internally by Heron Instance; instead, you should use this method to initialize
    any custom instance variables or connections to databases.

    *Should be implemented by a subclass.*

    :type config: dict
    :param config: The Heron configuration for this bolt. This is the configuration provided to
                   the topology merged in with cluster configuration on this machine.
                   Note that types of string values in the config have been automatically converted,
                   meaning that number strings and boolean strings are converted to appropriate
                   types.
    :type context: dict
    :param context: This object can be used to get information about this task's place within the
                    topology, including the task id and component id of this task, input and output
                    information, etc.
    """
    pass

  @abstractmethod
  def process(self, tup):
    """Process a single tuple of input

    The Tuple object contains metadata on it about which component/stream/task it came from.
    To emit a tuple, call ``self.emit(tuple)``.

    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**

    :type tup: heron.common.src.python.utils.tuple.HeronTuple
    :param tup: HeronTuple to process
    """
    raise NotImplementedError("Bolt not implementing process() method.")


  def cleanup(self):
    pass
