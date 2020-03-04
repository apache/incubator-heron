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

'''custom_grouping_helper.py'''
from collections import namedtuple

class CustomGroupingHelper(object):
  """Helper class for managing custom grouping"""
  def __init__(self):
    # map <stream_id -> list(targets)>
    self.targets = {}

  def add(self, stream_id, task_ids, grouping, source_comp_name):
    """Adds the target component

    :type stream_id: str
    :param stream_id: stream id into which tuples are emitted
    :type task_ids: list of str
    :param task_ids: list of task ids to which tuples are emitted
    :type grouping: ICustomStreamGrouping object
    :param grouping: custom grouping to use
    :type source_comp_name: str
    :param source_comp_name: source component name
    """
    if stream_id not in self.targets:
      self.targets[stream_id] = []
    self.targets[stream_id].append(Target(task_ids, grouping, source_comp_name))

  def prepare(self, context):
    """Prepares the custom grouping for this component"""
    for stream_id, targets in list(self.targets.items()):
      for target in targets:
        target.prepare(context, stream_id)

  def choose_tasks(self, stream_id, values):
    """Choose tasks for a given stream_id and values and Returns a list of target tasks"""
    if stream_id not in self.targets:
      return []

    ret = []
    for target in self.targets[stream_id]:
      ret.extend(target.choose_tasks(values))
    return ret

class Target(namedtuple('Target', 'task_ids, grouping, source_comp_name')):
  """Named tuple class for Target"""
  __slots__ = ()
  def prepare(self, context, stream_id):
    """Invoke prepare() of this custom grouping"""
    self.grouping.prepare(context, self.source_comp_name, stream_id, self.task_ids)

  def choose_tasks(self, values):
    """Invoke choose_tasks() of this custom grouping"""
    ret = self.grouping.choose_tasks(values)
    if not isinstance(ret, list):
      raise TypeError("Returned object after custom grouping's choose_tasks() "
                      "needs to be a list, given: %s" % str(type(ret)))
    else:
      for i in ret:
        if not isinstance(i, int):
          raise TypeError("Returned object after custom grouping's choose_tasks() "
                          "contained non-integer: %s" % str(i))
        if i not in self.task_ids:
          raise ValueError("Returned object after custom grouping's choose_tasks() contained "
                           "a task id that is not registered: %d" % i)
      return ret
