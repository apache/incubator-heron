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
"""module for map bolt: UnionBolt"""
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class UnionBolt(Bolt, StatefulComponent, StreamletBoltBase):
  """UnionBolt"""
  def init_state(self, stateful_state):
    # unionBolt does not have any state
    pass

  def pre_save(self, checkpoint_id):
    # unionBolt does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("UnionBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0

  def process(self, tup):
    self.emit([tup.values[0]], stream='output')
    self.processed += 1
    self.emitted += 1
    self.ack(tup)

# pylint: disable=protected-access
class UnionStreamlet(Streamlet):
  """UnionStreamlet"""
  def __init__(self, left, right):
    super(UnionStreamlet, self).__init__()
    if not isinstance(left, Streamlet):
      raise RuntimeError("Left of Union Streamlet has to be a Streamlet")
    if not isinstance(right, Streamlet):
      raise RuntimeError("Right of Union Streamlet has to be a Streamlet")
    self._left = left
    self._right = right
    self.set_num_partitions(left.get_num_partitions())

  def _calculate_inputs(self):
    return {GlobalStreamId(self._left.get_name(), self._left._output) :
            Grouping.SHUFFLE,
            GlobalStreamId(self._right.get_name(), self._right._output) :
            Grouping.SHUFFLE}

  def _build_this(self, builder, stage_names):
    if not self._left._built or not self._right._built:
      return False
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("union", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), UnionBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs())
    return True
