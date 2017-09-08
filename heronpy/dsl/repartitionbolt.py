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
"""module for map bolt: RepartitionBolt"""
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.dsl.streamlet import Streamlet
from heronpy.dsl.dslboltbase import DslBoltBase

# pylint: disable=unused-argument
class RepartitionBolt(Bolt, StatefulComponent, DslBoltBase):
  """RepartitionBolt"""

  def init_state(self, stateful_state):
    # repartition does not have any state
    pass

  def pre_save(self, checkpoint_id):
    # repartition does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("RepartitionBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0

  def process(self, tup):
    self.emit(tup.values, stream='output')
    self.processed += 1
    self.emitted += 1
    self.ack(tup)

# pylint: disable=protected-access
class RepartitionStreamlet(Streamlet):
  """RepartitionStreamlet"""
  def __init__(self, parallelism, parents, stage_name=None):
    super(RepartitionStreamlet, self).__init__(parents=parents,
                                               stage_name=stage_name, parallelism=parallelism)

  # pylint: disable=no-self-use
  def _calculate_inputs(self):
    return {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
            Grouping.SHUFFLE}

  def _calculate_stage_name(self, existing_stage_names):
    stagename = "repartition"
    if stagename not in existing_stage_names:
      return stagename
    else:
      index = 1
      newfuncname = stagename + str(index)
      while newfuncname in existing_stage_names:
        index = index + 1
        newfuncname = stagename + str(index)
      return newfuncname

  def _build_this(self, builder):
    builder.add_bolt(self._stage_name, RepartitionBolt, par=self._parallelism,
                     inputs=self._calculate_inputs())
