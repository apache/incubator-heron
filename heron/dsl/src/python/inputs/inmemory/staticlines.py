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
'''staticlines.py: module for defining a simple static lines input'''

from heron.api.src.python import Spout, Stream
from ...streamlet import Streamlet, OperationType

# pylint: disable=access-member-before-definition
# pylint: disable=attribute-defined-outside-init
class StaticLinesSpout(Spout):
  """StaticLinesSpout: Generates a line from a set of static lines again and again
  """
  outputs = [Stream(fields=['_output_'], name='output')]

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    """Implements StaticLines Spout's initialize method"""
    self.logger.info("Initializing StaticLinesSpout with the following")
    self.logger.info("Component-specific config: \n%s" % str(config))
    self.words = ["Mary had a little lamb",
                  "Humpy Dumpy sat on a wall",
                  "Here we round the Moulberry bush"]
    self.index = 0
    self.emit_count = 0
    self.ack_count = 0
    self.fail_count = 0

  def _get_next_line(self):
    retval = self.words[self.index]
    self.index += 1
    if self.index >= len(self.words):
      self.index = 0
    return retval

  def next_tuple(self):
    self.emit([self._get_next_line()], stream='output')
    self.emit_count += 1

  def ack(self, tup_id):
    self.ack_count += 1
    self.logger.debug("Acked tuple %s" % str(tup_id))

  def fail(self, tup_id):
    self.fail_count += 1
    self.logger.debug("Failed tuple %s" % str(tup_id))

class StaticLinesStreamlet(Streamlet):
  """A StaticLinesStreamlet spews a set of words forever
  """
  # pylint: disable=no-self-use
  def __init__(self, stage_name=None, parallelism=None):
    super(StaticLinesStreamlet, self).__init__(operation=OperationType.Input,
                                               stage_name=stage_name,
                                               parallelism=parallelism)

  @staticmethod
  def staticLinesGenerator(stage_name=None, parallelism=None):
    return StaticLinesStreamlet(stage_name=stage_name, parallelism=parallelism)

  def _build(self, bldr, stage_names):
    if self._parallelism is None:
      self._parallelism = 1
    if self._parallelism < 1:
      raise RuntimeError("StaticLines parallelism has to be >= 1")
    if self._stage_name is None:
      index = 1
      self._stage_name = "staticlines"
      while self._stage_name in stage_names:
        index = index + 1
        self._stage_name = "staticlines" + str(index)
      bldr.add_spout(self._stage_name, StaticLinesSpout, par=self._parallelism)
    return bldr
