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
from abc import abstractmethod

import Queue
import time
import collections

from .component import Component
from heron.proto import topology_pb2, tuple_pb2
from heron.common.src.python.log import Log
from heron.instance.src.python.instance.comp_spec import HeronComponentSpec
from heron.instance.src.python.instance.tuple import TupleHelper
from heron.instance.src.python.metrics.metrics_helper import SpoutMetrics

import heron.common.src.python.constants as constants

class Spout(Component):
  """The base class for all heron spouts in Python"""

  def __init__(self, pplan_helper, in_stream, out_stream, looper, sys_config):
    super(Spout, self).__init__(pplan_helper,in_stream, out_stream, looper, sys_config)
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

    if not self.pplan_helper.is_spout:
      raise RuntimeError("No spout in physicial plan")
    self.spout_config = self.pplan_helper.context['config']
    self.spout_metrics = SpoutMetrics(self.pplan_helper)

    # acking related
    self.acking_enabled = True if self.spout_config[constants.TOPOLOGY_ACKING_ENABLED] == "true" else False
    Log.info("Enable ACK: " + str(self.acking_enabled))
    self.in_flight_tuples = dict()
    self.immediate_acks = collections.deque()

    self.total_tuples_emitted = 0

    # TODO: topology context, serializer and sys config

  @classmethod
  def spec(cls, name=None, par=1, config=None):
    """Register this spout to the topology and create ``HeronComponentSpec``

    The usage of this method is compatible with StreamParse API, although it does not create
    ``ShellBoltSpec`` but instead directly registers to a ``Topology`` class.

    :type name: str
    :param name: Name of this spout.
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

    return HeronComponentSpec(name, python_class_path, is_spout=True, par=par,
                              inputs=None, outputs=_outputs, config=config)

  def start(self):
    # TODO: add config
    self.spout_metrics.register_metrics(self.pplan_helper.context, self.sys_config)
    self.initialize(config=self.spout_config, context=self.pplan_helper.context)
    self._add_spout_task()
    self.topology_state = topology_pb2.TopologyState.Value("RUNNING")

  def stop(self):
    pass

  def emit(self, tup, tup_id=None, stream=Component.DEFAULT_STREAM_ID,
           direct_task=None, need_task_ids=False):
    """Emits a new tuple from this Spout

    It is compatible with StreamParse API.

    :type tup: list or tuple
    :param tup: the new output Tuple to send from this spout, should contain only serializable data.
    :type tup_id: str or object
    :param tup_id: the ID for the Tuple. Leave this blank for an unreliable emit. (Same as messageId in Java)
    :type stream: str
    :param stream: the ID of the stream this Tuple should be emitted to. Leave empty to emit to the default stream.
    :type direct_task: int
    :param direct_task: the task to send the Tuple to if performing a direct emit.
    :type need_task_ids: bool
    :param need_task_ids: indicate whether or not you would like the task IDs the Tuple was emitted.
    """
    # TODO: check whether this tuple is sane with pplan_helper.check_output_schema
    # TODO: custom grouping and invoke hook emit
    data_tuple = tuple_pb2.HeronDataTuple()
    data_tuple.key = 0

    if tup_id is not None:
      tuple_info = TupleHelper.make_root_tuple_info(stream, tup_id)
      if self.acking_enabled:
        # this message is rooted
        root = data_tuple.roots.add()
        root.taskid = self.pplan_helper.my_task_id
        root.key = tuple_info.key
        self.in_flight_tuples[tuple_info.key] = tuple_info
      else:
        self.immediate_acks.append(tuple_info)

    tuple_size_in_bytes = 0

    # Serialize
    for obj in tup:
      serialized = self.serializer.serialize(obj)
      data_tuple.values.append(serialized)
      tuple_size_in_bytes += len(serialized)

    # TODO: return when need_task_ids=True
    ret = super(Spout, self).admit_data_tuple(stream_id=stream, data_tuple=data_tuple,
                                              tuple_size_in_bytes=tuple_size_in_bytes)

    self.spout_metrics.update_emit_count(stream)
    return ret

  def process_incoming_tuples(self):
    raise RuntimeError("Incoming tuple handling not implemented yet")

  def _read_tuples_and_execute(self):
    while not self.in_stream.is_empty():
      try:
        tuples = self.in_stream.poll()
      except Queue.Empty:
        break

      if isinstance(tuples, tuple_pb2.HeronTupleSet):
        if tuples.HasField("data"):
          raise RuntimeError("Spout cannot get incoming data tuples from other components")
        elif tuples.HasField("control"):
          for ack_tuple in tuples.control.acks:
            self._handle_ack_tuple(ack_tuple, True)
          for fail_tuple in tuples.control.fails:
            self._handle_ack_tuple(fail_tuple, False)
        else:
          Log.error("Received tuple neither data nor control")
      else:
        Log.error("Received tuple not instance of HeronTupleSet")

  def _handle_ack_tuple(self, tuple, is_success):
    # TODO: implement ACKs
    pass

  def _should_produce_tuple(self):
    # TODO: implement later -- like for Back Pressure
    return True

  def _produce_tuple(self):
    # TODO: do ACKing and stuff
    start_time = time.time()
    self.next_tuple()
    latency = time.time() - start_time
    self.spout_metrics.next_tuple(latency * constants.SEC_TO_NS)

  def _activate(self):
    Log.info("Spout is activated")
    self.activate()
    self.topology_state = topology_pb2.TopologyState.Value("RUNNING")

  def _deactivate(self):
    Log.info("Spout is deactivated")
    self.deactivate()
    self.topology_state = topology_pb2.TopologyState.Value("PAUSED")

  def _add_spout_task(self):
    #TODO: implement ACK/outqueue full etc
    #TODO: call _read_tuples_and_execute when ACK enabled

    Log.info("Add spout task")
    def spout_task():
      if self._should_produce_tuple():
        self._produce_tuple()
        self.output_helper.send_out_tuples()
        self.looper.wake_up() # so emitted tuples would be added to buffer now
      else:
        self.spout_metrics.update_out_queue_full_count()

      if self.acking_enabled:
        self._read_tuples_and_execute()
        # Add update pending tupels count for metrics
      else:
        self._do_immediate_acks()

      if self._is_continue_to_work():
        self.looper.wake_up()

    self.looper.add_wakeup_task(spout_task)

  def _do_immediate_acks(self):
    size = len(self.immediate_acks)
    for i in range(size):
      tuple_info = self.immediate_acks.pop()
      self._invoke_ack(tuple_info.tuple_id, tuple_info.stream_id, 0)

  def _invoke_ack(self, tuple_id, stream_id, complete_latency_ns):
    self.ack(tuple_id)
    self.spout_metrics.acked_tuple(stream_id, complete_latency_ns)

  ###################################
  # API: To be implemented by users
  ###################################

  @abstractmethod
  def initialize(self, config={}, context={}):
    """Called when a task for this component is initialized within a worker on the cluster

    It is compatible with StreamParse API. (Parameter name changed from ``storm_conf`` to ``config``)

    It provides the spout with the environment in which the spout executes. A good place to
    initialize connections to data sources.

    *Should be implemented by a subclass.*

    :type config: dict
    :param config: The Heron configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
    :type context: dict
    :param context: This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
    """
    pass

  def close(self):
    """Called when this spout is going to be shutdown

    There is no guarantee that close() will be called.
    """
    pass

  @abstractmethod
  def next_tuple(self):
    """When this method is called, Heron is requesting that the Spout emit tuples

    It is compatible with StreamParse API.

    This method should be non-blocking, so if the Spout has no tuples to emit,
    this method should return; next_tuple(), ack(), and fail() are all called in a tight
    loop in a single thread in the spout task. WHen there are no tuples to emit, it is
    courteous to have next_tuple sleep for a short amount of time (like a single millisecond)
    so as not to waste too much CPU.

    **Must be implemented by a subclass.**
    """
    raise NotImplementedError()

  def ack(self, tup_id):
    """Determine that the tuple emitted by this spout with the tup_id has been fully processed

    It is compatible with StreamParse API.

    Heron has determined that the tuple emitted by this spout with the tup_id identifier
    has been fully processed. Typically, an implementation of this method will take that
    message off the queue and prevent it from being replayed.

    *Should be implemented by a subclass.*
    """
    pass

  def fail(self, tup_id):
    """Determine that the tuple emitted by this spout with the tup_id has failed to be fully processed

    It is compatible with StreamParse API.

    The tuple emitted by this spout with the tup_id identifier has failed to be
    fully processed. Typically, an implementation of this method will put that
    message back on the queue to be replayed at a later time.

    *Should be implemented by a subclass.*
    """
    pass

  def activate(self):
    """Called when a spout has been activated out of a deactivated mode

    next_tuple() will be called on this spout soon. A spout can become activated
    after having been deactivated when the topology is manipulated using the
    `heron` client.
    """
    pass

  def deactivate(self):
    """Called when a spout has been deactivated

    next_tuple() will not be called while a spout is deactivated.
    The spout may or may not be reactivated in the future.
    """
    pass





