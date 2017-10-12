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
"""module for log bolt: LogBolt"""
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class LogBolt(Bolt, StatefulComponent, StreamletBoltBase):
  """LogBolt"""
  def init_state(self, stateful_state):
    # logBolt does not have any state
    pass

  def pre_save(self, checkpoint_id):
    # logBolt does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("LogBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0

  def process(self, tup):
    self.logger.info(str(tup.values[0]))
    self.processed += 1
    self.ack(tup)

# pylint: disable=protected-access
class LogStreamlet(Streamlet):
  """LogStreamlet"""
  def __init__(self, parent):
    super(LogStreamlet, self).__init__()
    if not isinstance(parent, Streamlet):
      raise RuntimeError("Parent of Log Streamlet has to be a Streamlet")
    self._parent = parent
    self.set_num_partitions(parent.get_num_partitions())

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parent.get_name(), self._parent._output) :
            Grouping.SHUFFLE}

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("log", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), LogBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs())
    return True
