# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''test_streamlet.py: module for running dsl in integration tests'''

from heronpy.dsl.streamlet import Streamlet
from .test_topology_builder import TestTopologyBuilder

class TestStreamlet(Streamlet):
  def __init__(self, streamlet):
    super(TestStreamlet, self).__init__(streamlet._parents, stage_name=streamlet._stage_name,
                                        parallelism=streamlet._parallelism)
    self._streamlet = streamlet

  def run(self, name, config=None, http_server_url=None):
    if name is None or not isinstance(name, str):
      raise RuntimeError("Job Name has to be a string")
    bldr = TestTopologyBuilder(name, http_server_url)
    stage_names = {}
    bldr = self._build(bldr, stage_names)
    if config is not None:
      if not isinstance(config, dict):
        raise RuntimeError("config has to be a dict")
      bldr.set_config(config)
    return bldr.create_topology()

  def _build(self, bldr, stage_names):
    for parent in self._parents:
      parent._build(bldr, stage_names)
    if self._parallelism is None:
      self._parallelism = self._calculate_parallelism()
      self._streamlet._parallelism = self._parallelism
    if self._stage_name is None:
      self._stage_name = self._calculate_stage_name(stage_names)
      self._streamlet._stage_name = self._stage_name
    if self._stage_name in stage_names:
      raise RuntimeError("duplicated stage name %s" % self._stage_name)
    stage_names[self._stage_name] = 1

    print "1 - self._stage_name: %s" % self._stage_name
    self._build_this(bldr)
    return bldr

  def _calculate_stage_name(self, existing_stage_names):
    stage_name = self._streamlet._calculate_stage_name(existing_stage_names)
    print "_calculate_stage_name: %s" % stage_name
    return stage_name

  def _build_this(self, builder):
    print "2 - self._stage_name: %s" % self._stage_name
    self._streamlet._build_this(builder)
