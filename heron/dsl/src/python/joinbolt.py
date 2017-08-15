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
"""module for join bolt: JoinBolt"""
import collections

from heron.api.src.python.stream import Stream
from heron.api.src.python.bolt.window_bolt import SlidingWindowBolt
from heron.api.src.python.component.component_spec import GlobalStreamId
from heron.api.src.python.custom_grouping import ICustomGrouping
from heron.api.src.python.stream import Grouping

from heron.dsl.src.python.streamlet import Streamlet, TimeWindow
from heron.dsl.src.python.operation import OperationType

# pylint: disable=unused-argument
class JoinBolt(SlidingWindowBolt):
  """JoinBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  WINDOWDURATION = SlidingWindowBolt.WINDOW_DURATION_SECS
  SLIDEINTERVAL = SlidingWindowBolt.WINDOW_SLIDEINTERVAL_SECS

  @staticmethod
  def _add(key, value, mymap):
    if key in mymap:
      mymap[key].append(value)
    else:
      mymap[key] = [value]

  def processWindow(self, window_config, tuples):
    # our temporary map
    mymap = {}
    for tup in tuples:
      userdata = tup.values[0]
      if not isinstance(userdata, collections.Iterable) or len(userdata) != 2:
        raise RuntimeError("Join tuples must be iterable of length 2")
      self._add(userdata[0], userdata[1], mymap)
    for (key, values) in mymap.items():
      self.emit([(key, values)], stream='output')

# pylint: disable=unused-argument
class JoinGrouping(ICustomGrouping):
  def prepare(self, context, component, stream, target_tasks):
    self.target_tasks = target_tasks

  def choose_tasks(self, values):
    assert isinstance(values, list) and len(values) == 1
    userdata = values[0]
    if not isinstance(userdata, collections.Iterable) or len(userdata) != 2:
      raise RuntimeError("Tuples going to join must be iterable of length 2")
    # only emits to the first task id
    hashvalue = hash(userdata[0])
    target_index = hashvalue % len(self.target_tasks)
    return [self.target_tasks[target_index]]

# pylint: disable=protected-access
class JoinStreamlet(Streamlet):
  """JoinStreamlet"""
  def __init__(self, time_window, parents, stage_name=None, parallelism=None):
    super(JoinStreamlet, self).__init__(parents=parents, operation=OperationType.Join,
                                        stage_name=stage_name, parallelism=parallelism)
    self._time_window = time_window

  def _calculate_inputs(self):
    inputs = {}
    for parent in self._parents:
      inputs[GlobalStreamId(parent._stage_name, parent._output)] = \
             Grouping.custom("heron.dsl.src.python.joinbolt.JoinGrouping")
    return inputs

  def _calculate_stage_name(self, existing_stage_names):
    stage_name = self._parents[0]._stage_name
    for stage in self._parents[1:]:
      stage_name = stage_name + '.join.' + stage._stage_name
    if stage_name not in existing_stage_names:
      return stage_name
    else:
      index = 1
      tmp_name = stage_name + str(index)
      while tmp_name in existing_stage_names:
        index = index + 1
        tmp_name = stage_name + str(index)
      return tmp_name

  def _build_this(self, builder):
    if not isinstance(self._time_window, TimeWindow):
      raise RuntimeError("Join's time_window should be TimeWindow")
    builder.add_bolt(self._stage_name, JoinBolt, par=self._parallelism,
                     inputs=self._inputs,
                     config={JoinBolt.WINDOWDURATION : self._time_window.duration,
                             JoinBolt.SLIDEINTERVAL : self._time_window.sliding_interval})
