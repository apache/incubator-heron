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
'''module for example task hook'''
from collections import Counter

from heron.common.src.python.utils.log import Log
from heron.common.src.python.utils.topology import ITaskHook

# pylint: disable=unused-argument
class TestTaskHook(ITaskHook):
  """TestTaskHook logs event information every 10000 times"""
  CONST = 10000

  def prepare(self, conf, context):
    Log.info("In prepare of TestTaskHook")
    self.counter = Counter()

  # pylint: disable=no-self-use
  def clean_up(self):
    Log.info("In clean_up of TestTaskHook")

  def emit(self, emit_info):
    self.counter['emit'] += 1
    if self.counter['emit'] % self.CONST == 0:
      Log.info("TestTaskHook: emitted %s tuples" % str(self.counter['emit']))

  def spout_ack(self, spout_ack_info):
    self.counter['sp_ack'] += 1
    if self.counter['sp_ack'] % self.CONST == 0:
      Log.info("TestTaskHook: spout acked %s tuples" % str(self.counter['sp_ack']))

  def spout_fail(self, spout_fail_info):
    self.counter['sp_fail'] += 1
    if self.counter['sp_fail'] % self.CONST == 0:
      Log.info("TestTaskHook: spout failed %s tuples" % str(self.counter['sp_fail']))

  def bolt_execute(self, bolt_execute_info):
    self.counter['bl_exec'] += 1
    if self.counter['bl_exec'] % self.CONST == 0:
      Log.info("TestTaskHook: bolt executed %s tuples" % str(self.counter['bl_exec']))

  def bolt_ack(self, bolt_ack_info):
    self.counter['bl_ack'] += 1
    if self.counter['bl_ack'] % self.CONST == 0:
      Log.info("TestTaskHook: bolt acked %s tuples" % str(self.counter['bl_ack']))

  def bolt_fail(self, bolt_fail_info):
    self.counter['bl_fail'] += 1
    if self.counter['bl_fail'] % self.CONST == 0:
      Log.info("TestTaskHook: bolt failed %s tuples" % str(self.counter['bl_fail']))
