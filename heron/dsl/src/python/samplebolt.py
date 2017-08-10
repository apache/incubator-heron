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
"""module for sample bolt: SampleBolt
   SampleBolt is a more sophisticated FilterBolt which
   can do sampling of the data that it recieves and emit
   only sampled tuples"""
from heron.api.src.python.bolt.bolt import Bolt
from heron.api.src.python.stream import Stream
from heron.api.src.python.state.stateful_component import StatefulComponent
from heron.api.src.python.component.component_spec import GlobalStreamId
from heron.api.src.python.stream import Grouping

from heron.dsl.src.python.streamlet import Streamlet
from heron.dsl.src.python.operation import OperationType

# pylint: disable=unused-argument
class SampleBolt(Bolt, StatefulComponent):
  """SampleBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  FRACTION = 'fraction'

  def initState(self, stateful_state):
    # sample does not have any state
    pass

  def preSave(self, checkpoint_id):
    # sample does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("SampleBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if SampleBolt.FRACTION in config:
      self.sample_fraction = config[SampleBolt.FRACTION]
      if not isinstance(self.sample_fraction, float):
        raise RuntimeError("Sample fraction has to be a float")
      if self.sample_fraction > 1.0:
        raise RuntimeError("Sample fraction has to be <= 1.0")
    else:
      raise RuntimeError("SampleBolt needs to be passed filter function")

  def process(self, tup):
    self.processed += 1
    self.ack(tup)
    raise RuntimeError("SampleBolt not fully functional")

# pylint: disable=protected-access
class SampleStreamlet(Streamlet):
  """SampleStreamlet"""
  def __init__(self, sample_fraction, parents, stage_name=None, parallelism=None):
    super(SampleStreamlet, self).__init__(parents=parents, operation=OperationType.Sample,
                                          stage_name=stage_name, parallelism=parallelism)
    self._sample_fraction = sample_fraction

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
            Grouping.SHUFFLE}

  def _calculate_stage_name(self, existing_stage_names):
    funcname = "sample-" + self._sample_function.__name__
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
    if not isinstance(self._sample_fraction, float):
      raise RuntimeError("Sample Fraction has to be a float")
    if self._sample_fraction > 1.0:
      raise RuntimeError("Sample Fraction has to be <= 1.0")
    builder.add_bolt(self._stage_name, SampleBolt, par=self._parallelism,
                     inputs=self._inputs,
                     config={SampleBolt.FRACTION : self._sample_fraction})
