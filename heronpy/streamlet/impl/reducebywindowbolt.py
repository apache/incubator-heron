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
"""module for bolt: ReduceByWindowBolt"""
import collections

from heronpy.api.bolt.window_bolt import SlidingWindowBolt
from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.window import Window
from heronpy.streamlet.windowconfig import WindowConfig
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class ReduceByWindowBolt(SlidingWindowBolt, StreamletBoltBase):
  """ReduceByWindowBolt"""
  FUNCTION = 'function'
  WINDOWDURATION = SlidingWindowBolt.WINDOW_DURATION_SECS
  SLIDEINTERVAL = SlidingWindowBolt.WINDOW_SLIDEINTERVAL_SECS

  def initialize(self, config, context):
    super(ReduceByWindowBolt, self).initialize(config, context)
    if ReduceByWindowBolt.FUNCTION not in config:
      raise RuntimeError("FUNCTION not specified in reducebywindow operator")
    self.reduce_function = config[ReduceByWindowBolt.FUNCTION]

  def processWindow(self, window_config, tuples):
    result = None
    for tup in tuples:
      userdata = tup.values[0]
      result = self.reduce_function(result, userdata)
    self.emit([(Window(window_config.start, window_config.end), result)], stream='output')

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
class ReduceByWindowStreamlet(Streamlet):
  """ReduceByWindowStreamlet"""
  def __init__(self, window_config, reduce_function, parent):
    super(ReduceByWindowStreamlet, self).__init__()
    if not isinstance(window_config, WindowConfig):
      raise RuntimeError("window config has to be of type WindowConfig")
    if not callable(reduce_function):
      raise RuntimeError("ReduceByWindow function has to be callable")
    if not isinstance(parent, Streamlet):
      raise RuntimeError("Parent of FlatMap Streamlet has to be a Streamlet")
    self._parent = parent
    self._window_config = window_config
    self._reduce_function = reduce_function
    self.set_num_partitions(parent.get_num_partitions())

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parent.get_name(), self._parent._output) :
            Grouping.custom("heronpy.streamlet.impl.reducebywindowbolt.ReduceGrouping")}

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("reducebywindow", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), ReduceByWindowBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs(),
                     config={ReduceByWindowBolt.FUNCTION : self._reduce_function,
                             ReduceByWindowBolt.WINDOWDURATION :
                             self._window_config._window_duration.seconds,
                             ReduceByWindowBolt.SLIDEINTERVAL :
                             self._window_config._slide_interval.seconds})
    return True
