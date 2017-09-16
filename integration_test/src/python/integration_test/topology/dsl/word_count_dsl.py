# copyright 2016 twitter. all rights reserved.
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
'''Example WordCountTopology'''

from heronpy.dsl.streamlet import TimeWindow, Streamlet
from heronpy.api.spout.spout import Spout
from heronpy.dsl.dslboltbase import DslBoltBase
from integration_test.src.python.integration_test.core.test_streamlet import TestStreamlet
import logging
import time

def word_count_dsl_builder(topology_name, http_server_url):
  counts = TestStreamlet(FixedLinesStreamlet.fixedLinesGenerator(parallelism=1) \
    .flat_map(lambda line: line.split(), parallelism=1) \
    .map(lambda word: (word, 1), parallelism=1) \
    .reduce_by_window(TimeWindow(5, 5), lambda x, y: x + y))
  return counts.run(topology_name, http_server_url=http_server_url)

class FixedLinesStreamlet(Streamlet):
  """A FixedLinesStreamlet spews a set of words forever
  """
  # pylint: disable=no-self-use
  def __init__(self, stage_name=None, parallelism=None):
    super(FixedLinesStreamlet, self).__init__(parents=[],
                                              stage_name=stage_name,
                                              parallelism=parallelism)

  @staticmethod
  def fixedLinesGenerator(stage_name=None, parallelism=None):
    return FixedLinesStreamlet(stage_name=stage_name, parallelism=parallelism)

  def _calculate_stage_name(self, existing_stage_names):
    stagename = "fixedlines"
    if stagename not in existing_stage_names:
      return stagename
    else:
      index = 1
      newname = stagename + str(index)
      while newname in existing_stage_names:
        index = index + 1
        newname = stagename + str(index)
      return newname

  def _build_this(self, bldr):
    bldr.add_spout(self._stage_name, FixedLinesSpout, par=self._parallelism)

class FixedLinesSpout(Spout, DslBoltBase):
  """FixedLinesSpout: Generates a line from a set of static lines again and again
  """
  # pylint: disable=unused-argument
  def initialize(self, config, context):
    """Implements FixedLines Spout's initialize method"""
    self.logger.info("Initializing FixedLinesSpout with the following")
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
    time.sleep(1)
    self.emit([self._get_next_line()], stream='output')
    self.emit_count += 1

  def ack(self, tup_id):
    self.ack_count += 1
    self.logger.debug("Acked tuple %s" % str(tup_id))

  def fail(self, tup_id):
    self.fail_count += 1
    self.logger.debug("Failed tuple %s" % str(tup_id))