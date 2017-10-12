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
"""module for flat_map bolt: FlatMapBolt"""
import collections
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class FlatMapBolt(Bolt, StatefulComponent, StreamletBoltBase):
  """FlatMapBolt"""
  FUNCTION = 'function'

  def init_state(self, stateful_state):
    # flat_map does not have any state
    pass

  def pre_save(self, checkpoint_id):
    # flat_map does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("FlatMapBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if FlatMapBolt.FUNCTION in config:
      self.flatmap_function = config[FlatMapBolt.FUNCTION]
    else:
      raise RuntimeError("FlatMapBolt needs to be passed flat_map function")

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
  def __init__(self, flatmap_function, parent):
    super(FlatMapStreamlet, self).__init__()
    if not callable(flatmap_function):
      raise RuntimeError("FlatMap function has to be callable")
    if not isinstance(parent, Streamlet):
      raise RuntimeError("Parent of FlatMap Streamlet has to be a Streamlet")
    self._parent = parent
    self._flatmap_function = flatmap_function
    self.set_num_partitions(parent.get_num_partitions())

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parent.get_name(), self._parent._output) :
            Grouping.SHUFFLE}

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("flatmap", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), FlatMapBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs(),
                     config={FlatMapBolt.FUNCTION : self._flatmap_function})
    return True
