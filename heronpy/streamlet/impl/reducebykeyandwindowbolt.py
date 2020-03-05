#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""module for join bolt: ReduceByKeyAndWindowBolt"""
import collections

from heronpy.api.bolt.window_bolt import SlidingWindowBolt
from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.keyedwindow import KeyedWindow
from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.window import Window
from heronpy.streamlet.windowconfig import WindowConfig
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class ReduceByKeyAndWindowBolt(SlidingWindowBolt, StreamletBoltBase):
  """ReduceByKeyAndWindowBolt"""
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
    for (key, values) in list(mymap.items()):
      result = values[0]
      for value in values[1:]:
        result = self.reduce_function(result, value)
      keyedwindow = KeyedWindow(key, Window(window_config.start, window_config.end))
      self.emit([(keyedwindow, result)], stream='output')

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
  def __init__(self, window_config, reduce_function, parent):
    super(ReduceByKeyAndWindowStreamlet, self).__init__()
    if not isinstance(window_config, WindowConfig):
      raise RuntimeError("window config has to be a WindowConfig")
    if not callable(reduce_function):
      raise RuntimeError("ReduceByKeyAndWindow function has to be callable")
    if not isinstance(parent, Streamlet):
      raise RuntimeError("Parent of Filter Streamlet has to be a Streamlet")
    self._window_config = window_config
    self._reduce_function = reduce_function
    self._parent = parent

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parent.get_name(), self._parent._output) :
            Grouping.custom("heronpy.streamlet.impl.reducebykeyandwindowbolt.ReduceGrouping")}

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("reducebykeyandwindow", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), ReduceByKeyAndWindowBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs(),
                     config={ReduceByKeyAndWindowBolt.FUNCTION : self._reduce_function,
                             ReduceByKeyAndWindowBolt.WINDOWDURATION :
                             self._window_config._window_duration.seconds,
                             ReduceByKeyAndWindowBolt.SLIDEINTERVAL :
                             self._window_config._slide_interval.seconds})
    return True
