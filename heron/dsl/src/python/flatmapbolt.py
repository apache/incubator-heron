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
"""module for flatMap bolt: FlatMapBolt"""
import collections
from heron.api.src.python.bolt.bolt import Bolt
from heron.api.src.python.stream import Stream
from heron.api.src.python.state.stateful_component import StatefulComponent
from heron.api.src.python.component.component_spec import GlobalStreamId
from heron.api.src.python.stream import Grouping

from heron.dsl.src.python.streamlet import Streamlet
from heron.dsl.src.python.operation import OperationType

# pylint: disable=unused-argument
class FlatMapBolt(Bolt, StatefulComponent):
  """FlatMapBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  FUNCTION = 'function'

  def initState(self, stateful_state):
    # flatMap does not have any state
    pass

  def preSave(self, checkpoint_id):
    # flatMap does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("FlatMapBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if FlatMapBolt.FUNCTION in config:
      self.flatmap_function = config[FlatMapBolt.FUNCTION]
      if not callable(self.flatmap_function):
        raise RuntimeError("FlatMap function has to be callable")
    else:
      raise RuntimeError("FlatMapBolt needs to be passed flatMap function")

  def process(self, tup):
    retval = self.flatmap_function(tup.values[0])
    if isinstance(retval, collections.Iterable):
      for value in retval:
        self.emit([value], stream='output')
        self.emitted += 1
    else:
      self.emit([retval], stream='output')
      self.emitted += 1
    self.processed += 1
    self.ack(tup)

# pylint: disable=protected-access
class FlatMapStreamlet(Streamlet):
  """FlatMapStreamlet"""
  def __init__(self, flatmap_function, parents, stage_name=None, parallelism=None):
    super(FlatMapStreamlet, self).__init__(parents=parents, operation=OperationType.FlatMap,
                                           stage_name=stage_name, parallelism=parallelism)
    self._flatmap_function = flatmap_function

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
            Grouping.SHUFFLE}

  def _calculate_stage_name(self, existing_stage_names):
    funcname = "flatmap-" + self._flatmap_function.__name__
    if funcname not in existing_stage_names:
      return funcname
    else:
      index = 1
      newfuncname = funcname + str(index)
      while newfuncname in existing_stage_names:
        index = index + 1
        newfuncname = funcname + str(index)
      return newfuncname

  def _build_this(self, builder):
    if not callable(self._flatmap_function):
      raise RuntimeError("flatmap function must be callable")
    builder.add_bolt(self._stage_name, FlatMapBolt, par=self._parallelism,
                     inputs=self._inputs,
                     config={FlatMapBolt.FUNCTION : self._flatmap_function})
