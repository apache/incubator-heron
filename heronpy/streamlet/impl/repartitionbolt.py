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
import collections
import inspect

from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class RepartitionCustomGrouping(ICustomGrouping):
  def __init__(self, repartition_function):
    self._repartition_function = repartition_function

  def prepare(self, context, component, stream, target_tasks):
    self.logger.info("In prepare of SampleCustomGrouping, "
                     "with src component: %s, "
                     "with stream id: %s, "
                     "with target tasks: %s"
                     , component, stream, str(target_tasks))
    self.target_tasks = target_tasks

  def choose_tasks(self, values):
    # only emits to the first task id
    targets = self._repartition_function(values, len(self.target_tasks))
    retval = []
    if isinstance(targets, collections.Iterable):
      for target in targets:
        retval.append(self.target_tasks[target % len(self.target_tasks)])
    else:
      retval.append(self.target_tasks[targets % len(self.target_tasks)])
    return retval

# pylint: disable=unused-argument
class RepartitionBolt(Bolt, StatefulComponent, StreamletBoltBase):
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
    self.emit([tup.values[0]], stream='output')
    self.processed += 1
    self.emitted += 1
    self.ack(tup)

# pylint: disable=protected-access,deprecated-method
class RepartitionStreamlet(Streamlet):
  """RepartitionStreamlet"""
  def __init__(self, num_partitions, repartition_function, parent):
    super(RepartitionStreamlet, self).__init__()
    if not callable(repartition_function):
      raise RuntimeError("Repartition function has to be callable")
    if len(inspect.getargspec(repartition_function)) != 2:
      raise RuntimeError("Repartition function should take 2 arguments")
    if not isinstance(parent, Streamlet):
      raise RuntimeError("Parent of FlatMap Streamlet has to be a Streamlet")
    self._parent = parent
    self._repartition_function = repartition_function
    self.set_num_partitions(num_partitions)

  # pylint: disable=no-self-use
  def _calculate_inputs(self):
    return {GlobalStreamId(self._parent.get_name(), self._parent._output) :
            Grouping.custom(RepartitionCustomGrouping(self._repartition_function))}

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("repartition", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), RepartitionBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs())
    return True
