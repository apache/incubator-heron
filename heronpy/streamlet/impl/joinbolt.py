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

"""module for join bolt: JoinBolt"""
import collections

from heronpy.api.bolt.window_bolt import SlidingWindowBolt
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.api.stream import Grouping

from heronpy.streamlet.keyedwindow import KeyedWindow
from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.window import Window
from heronpy.streamlet.windowconfig import WindowConfig
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase


# pylint: disable=unused-argument
# pylint: disable=too-many-branches
class JoinBolt(SlidingWindowBolt, StreamletBoltBase):
  """JoinBolt"""

  OUTER_LEFT = 1
  INNER = 2
  OUTER_RIGHT = 3
  OUTER = 4
  JOINFUNCTION = '__join_function__'
  JOINTYPE = '__join_type__'
  WINDOWDURATION = SlidingWindowBolt.WINDOW_DURATION_SECS
  SLIDEINTERVAL = SlidingWindowBolt.WINDOW_SLIDEINTERVAL_SECS
  JOINEDCOMPONENT = '__joined_component__'

  def _add(self, key, value, src_component, mymap):
    if not key in mymap:
      mymap[key] = ([], [])
    # Join Output should be Key -> (V1, V2) where
    # V1 is coming from the left stream and V2 coming
    # from the right stream. In this case, _joined_component
    # represents the right stream
    if src_component == self._joined_component:
      mymap[key][1].append(value)
    else:
      mymap[key][0].append(value)

  def initialize(self, config, context):
    super(JoinBolt, self).initialize(config, context)
    if not JoinBolt.JOINEDCOMPONENT in config:
      raise RuntimeError("%s must be specified in the JoinBolt" % JoinBolt.JOINEDCOMPONENT)
    self._joined_component = config[JoinBolt.JOINEDCOMPONENT]
    if not JoinBolt.JOINFUNCTION in config:
      raise RuntimeError("%s must be specified in the JoinBolt" % JoinBolt.JOINFUNCTION)
    self._join_function = config[JoinBolt.JOINFUNCTION]
    if not JoinBolt.JOINTYPE in config:
      raise RuntimeError("%s must be specified in the JoinBolt" % JoinBolt.JOINTYPE)
    self._join_type = config[JoinBolt.JOINTYPE]

  def processWindow(self, window_config, tuples):
    # our temporary map
    mymap = {}
    for tup in tuples:
      userdata = tup.values[0]
      if not isinstance(userdata, collections.Iterable) or len(userdata) != 2:
        raise RuntimeError("Join tuples must be iterable of length 2")
      self._add(userdata[0], userdata[1], tup.component, mymap)
    for (key, values) in list(mymap.items()):
      if self._join_type == JoinBolt.INNER:
        if values[0] and values[1]:
          self.inner_join_and_emit(key, values, window_config)
      elif self._join_type == JoinBolt.OUTER_LEFT:
        if values[0] and values[1]:
          self.inner_join_and_emit(key, values, window_config)
        elif values[0]:
          self.outer_left_join_and_emit(key, values, window_config)
      elif self._join_type == JoinBolt.OUTER_RIGHT:
        if values[0] and values[1]:
          self.inner_join_and_emit(key, values, window_config)
        elif values[1]:
          self.outer_right_join_and_emit(key, values, window_config)
      elif self._join_type == JoinBolt.OUTER:
        if values[0] and values[1]:
          self.inner_join_and_emit(key, values, window_config)
        elif values[0]:
          self.outer_left_join_and_emit(key, values, window_config)
        elif values[1]:
          self.outer_right_join_and_emit(key, values, window_config)

  def emit_join(self, key, values, window_config):
    result = self._join_function(values[0], values[1])
    keyedwindow = KeyedWindow(key, Window(window_config.start, window_config.end))
    self.emit([(keyedwindow, result)], stream='output')

  def inner_join_and_emit(self, key, values, window_config):
    for left_val in values[0]:
      for right_val in values[1]:
        result = self._join_function(left_val, right_val)
        keyedwindow = KeyedWindow(key, Window(window_config.start, window_config.end))
        self.emit([(keyedwindow, result)], stream='output')

  def outer_left_join_and_emit(self, key, values, window_config):
    for left_val in values[0]:
      result = self._join_function(left_val, None)
      keyedwindow = KeyedWindow(key, Window(window_config.start, window_config.end))
      self.emit([(keyedwindow, result)], stream='output')

  def outer_right_join_and_emit(self, key, values, window_config):
    for right_val in values[1]:
      result = self._join_function(None, right_val)
      keyedwindow = KeyedWindow(key, Window(window_config.start, window_config.end))
      self.emit([(keyedwindow, result)], stream='output')


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
  def __init__(self, join_type, window_config, join_function, left, right):
    super(JoinStreamlet, self).__init__()
    if not join_type in [JoinBolt.INNER, JoinBolt.OUTER_RIGHT, JoinBolt.OUTER_LEFT]:
      raise RuntimeError("join type has to be of one of inner, outer, left")
    if not isinstance(window_config, WindowConfig):
      raise RuntimeError("window config has to be of type WindowConfig")
    if not callable(join_function):
      raise RuntimeError("Join function has to be callable")
    if not isinstance(left, Streamlet):
      raise RuntimeError("Parent of Join has to be a Streamlet")
    if not isinstance(right, Streamlet):
      raise RuntimeError("Parent of Join has to be a Streamlet")
    self._join_type = join_type
    self._window_config = window_config
    self._join_function = join_function
    self._left = left
    self._right = right
    self.set_num_partitions(left.get_num_partitions())

  def _calculate_inputs(self):
    return {GlobalStreamId(self._left.get_name(), self._left._output) :
            Grouping.custom("heronpy.streamlet.impl.joinbolt.JoinGrouping"),
            GlobalStreamId(self._right.get_name(), self._right._output) :
            Grouping.custom("heronpy.streamlet.impl.joinbolt.JoinGrouping")}

  # pylint: disable=superfluous-parens
  def _build_this(self, builder, stage_names):
    print("join_build_this left: %s right: %s" % (self._left._built, self._right._built))
    print("left: %s right: %s" % (self._left.get_name(), self._right.get_name()))
    if not self._left._built or not self._right._built:
      return False
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("join", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), JoinBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs(),
                     config={JoinBolt.WINDOWDURATION : self._window_config._window_duration.seconds,
                             JoinBolt.SLIDEINTERVAL : self._window_config._slide_interval.seconds,
                             JoinBolt.JOINEDCOMPONENT : self._right.get_name(),
                             JoinBolt.JOINFUNCTION : self._join_function,
                             JoinBolt.JOINTYPE : self._join_type})
    return True
