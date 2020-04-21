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

"""module for contextimpl: ContextImpl"""

from heronpy.streamlet.context import Context

class ContextImpl(Context):
  """ContextImpl"""
  def __init__(self, top_context, state, emitter):
    self._top_context = top_context
    self._state = state
    self._emitter = emitter

  def get_task_id(self):
    return self._top_context.get_task_id()

  def get_config(self):
    return self._top_context.get_cluster_config()

  def get_stream_name(self):
    return list(self._top_context.get_this_sources().keys())[0].id

  def get_num_partitions(self):
    return len(self._top_context.get_component_tasks(self._top_context.get_component_id()))

  def get_partition_index(self):
    tasks = self._top_context.get_component_tasks(self._top_context.get_component_id())
    tasks.sort()
    return tasks.index(self.get_task_id())

  def get_state(self):
    return self._state

  def emit(self, values):
    self._emitter.emit([values], stream='output')
