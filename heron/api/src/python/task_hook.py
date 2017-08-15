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
'''task_hook.py: modules for supporting task hooks for topology'''
from collections import namedtuple
from abc import abstractmethod

class ITaskHook(object):
  """ITaskHook is an interface for defining task hooks for a topology"""

  @abstractmethod
  def prepare(self, conf, context):
    """Called after the spout/bolt's initialize() method is called

    :param conf: component-specific configuration passed to the topology
    :param context: topology context
    """
    pass

  @abstractmethod
  def clean_up(self):
    """Called just before the spout/bolt's cleanup method is called"""
    pass

  @abstractmethod
  def emit(self, emit_info):
    """Called every time a tuple is emitted in spout/bolt

    :param emit_info: EmitInfo object
    """
    pass

  @abstractmethod
  def spout_ack(self, spout_ack_info):
    """Called in spout every time a tuple gets acked

    :param spout_ack_info: SpoutAckInfo object
    """
    pass

  @abstractmethod
  def spout_fail(self, spout_fail_info):
    """Called in spout every time a tuple gets failed

    :param spout_fail_info: SpoutFailInfo object
    """
    pass

  @abstractmethod
  def bolt_execute(self, bolt_execute_info):
    """Called in bolt every time a tuple gets executed

    :param bolt_execute_info: BoltExecuteInfo object
    """
    pass

  @abstractmethod
  def bolt_ack(self, bolt_ack_info):
    """Called in bolt every time a tuple gets acked

    :param bolt_ack_info: BoltAckInfo object
    """
    pass

  @abstractmethod
  def bolt_fail(self, bolt_fail_info):
    """Called in bolt every time a tuple gets failed

    :param bolt_fail_info: BoltFailInfo object
    """
    pass


##################################################################################
## Below are named tuples for each information                                  ##
## Topology writers never need to create an instance of the following classes,  ##
## as they are automatically created by the Heron Instance                      ##
##################################################################################

EmitInfo = namedtuple('EmitInfo', 'values, stream_id, task_id, out_tasks')
"""Information about emit

:ivar values: (list) values emitted
:ivar stream_id: (str) stream id into which tuple is emitted
:ivar task_id: (int) task id on which emit() was called
:ivar out_tasks: (list) list of custom grouping target task id
"""

SpoutAckInfo = namedtuple('SpoutAckInfo', 'message_id, spout_task_id, complete_latency_ms')
"""Information about Spout's Acking of a tuple

:ivar message_id: message id to which an acked tuple was anchored
:ivar spout_task_id: (int) task id of spout
:ivar complete_latency_ms: (float) complete latency in ms
"""

SpoutFailInfo = namedtuple('SpoutFailInfo', 'message_id, spout_task_id, fail_latency_ms')
"""Information about Spout's Failing of a tuple

:ivar message_id: message id to which an acked tuple was anchored
:ivar spout_task_id: (int) task id of spout
:ivar fail_latency_ms: (float) fail latency in ms
"""

BoltExecuteInfo = \
  namedtuple('BoltExecuteInfo', 'heron_tuple, executing_task_id, execute_latency_ms')
"""Information about Bolt's executing of a tuple

:ivar heron_tuple: (HeronTuple) tuple that is executed
:ivar executing_task_id: (int) task id of bolt
:ivar execute_latency_ms: (float) execute latency in ms
"""

BoltAckInfo = namedtuple('BoltAckInfo', 'heron_tuple, acking_task_id, process_latency_ms')
"""Information about Bolt's acking of a tuple

:ivar heron_tuple: (HeronTuple) tuple that is acked
:ivar acking_task_id: (int) task id of bolt
:ivar process_latency_ms: (float) process latency in ms
  """

BoltFailInfo = namedtuple('BoltFailInfo', 'heron_tuple, failing_task_id, fail_latency_ms')
"""Information about Bolt's failing of a tuple

:ivar heron_tuple: (HeronTuple) tuple that is failed
:ivar failing_task_id: (int) task id of bolt
:ivar fail_latency_ms: (float) fail latency in ms
"""
