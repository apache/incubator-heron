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
"""module for join bolt: ReduceByKeyAndWindowBolt"""
import collections

from heron.api.src.python.stream import Stream
from heron.api.src.python.bolt.window_bolt import SlidingWindowBolt
from heron.api.src.python.custom_grouping import ICustomGrouping
from heron.api.src.python.component.component_spec import GlobalStreamId
from heron.api.src.python.stream import Grouping

from heron.dsl.src.python.streamlet import Streamlet, TimeWindow
from heron.dsl.src.python.operation import OperationType

# pylint: disable=unused-argument
class ReduceByKeyAndWindowBolt(SlidingWindowBolt):
  """ReduceByKeyAndWindowBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  FUNCTION = 'function'
  WINDOWDURATION = SlidingWindowBolt.WINDOW_DURATION_SECS
  SLIDEINTERVAL = SlidingWindowBolt.WINDOW_SLIDEINTERVAL_SECS

  def initialize(self, config, context):
    super(ReduceByKeyAndWindowBolt, self).initialize(config, context)
    if ReduceByKeyAndWindowBolt.FUNCTION not in config:
      raise RuntimeError("FUNCTION not specified in reducebywindow operator")
    self.reduce_function = config[ReduceByKeyAndWindowBolt.FUNCTION]
    if not callable(self.reduce_function):
      raise RuntimeError("Reduce Function has to be callable")

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
        raise RuntimeError("ReduceByWindow tuples must be iterable of length 2")
      self._add(userdata[0], userdata[1], mymap)
    for (key, values) in mymap.items():
      result = values[0]
      for value in values[1:]:
        self.reduce_function(result, value)
      self.emit([(key, result)], stream='output')

# pylint: disable=unused-argument
class ReduceGrouping(ICustomGrouping):
  def prepare(self, context, component, stream, target_tasks):
    self.target_tasks = target_tasks

  def choose_tasks(self, values):
    assert isinstance(values, list) and len(values) == 1
    userdata = values[0]
    if not isinstance(userdata, collections.Iterable) or len(userdata) != 2:
      raise RuntimeError("Tuples going to reduce must be iterable of length 2")
    # only emits to the first task id
    hashvalue = hash(userdata[0])
    target_index = hashvalue % len(self.target_tasks)
    return [self.target_tasks[target_index]]

# pylint: disable=protected-access
class ReduceByKeyAndWindowStreamlet(Streamlet):
  """ReduceByKeyAndWindowStreamlet"""
  def __init__(self, time_window, reduce_function, parents, stage_name=None, parallelism=None):
    op = OperationType.ReduceByKeyAndWindow
    super(ReduceByKeyAndWindowStreamlet, self).__init__(parents=parents,
                                                        operation=op,
                                                        stage_name=stage_name,
                                                        parallelism=parallelism)
    self._time_window = time_window
    self._reduce_function = reduce_function

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
            Grouping.custom("heron.dsl.src.python.reducebykeyandwindowbolt.ReduceGrouping")}

  def _calculate_stage_name(self, existing_stage_names):
    funcname = "reducebykeyandwindow-" + self._reduce_function.__name__
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
    if not callable(self._reduce_function):
      raise RuntimeError("reduce function must be callable")
    if not isinstance(self._time_window, TimeWindow):
      raise RuntimeError("reduce's time_window should be TimeWindow")
    builder.add_bolt(self._stage_name, ReduceByKeyAndWindowBolt, par=self._parallelism,
                     inputs=self._inputs,
                     config={ReduceByKeyAndWindowBolt.FUNCTION : self._reduce_function,
                             ReduceByKeyAndWindowBolt.WINDOWDURATION : self._time_window.duration,
                             ReduceByKeyAndWindowBolt.SLIDEINTERVAL :
                             self._time_window.sliding_interval})
