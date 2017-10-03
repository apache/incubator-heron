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
"""module for contextimpl: ContextImpl"""

from heronpy.dsl.context import Context

class ContextImpl(Context):
  """ContextImpl"""
  def __init__(self, topology_context, state):
    self._topology_context = topology_context
    self._state = state

  def get_task_id(self):
    return self._topology_context.get_task_id()

  def get_config(self):
    return self._topology_context.get_cluster_config()

  def get_stream_name(self):
    return self._topology_context.get_this_sources().keys()[0].id

  def get_state(self):
    return self._state
