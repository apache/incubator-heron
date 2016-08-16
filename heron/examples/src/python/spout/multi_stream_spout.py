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
"""Word spout with error streams"""

from itertools import cycle
from heron.pyheron.src.python import Spout, Stream

class MultiStreamSpout(Spout):
  """WordSpout: emits a set of words repeatedly"""
  # output field declarer
  outputs = ['word', Stream(fields=['error_msg'], name='error')]

  def initialize(self, config, context):
    self.logger.info("In initialize() of WordSpout")
    self.words = cycle(["hello", "bye", "good", "bad", "heron", "storm"])

    self.emit_count = 0

    self.logger.info("Component-specific config: \n%s" % str(config))
    self.logger.info("Context: \n%s" % str(context))

  def next_tuple(self):
    word = next(self.words)
    self.emit([word])
    self.emit_count += 1

    if self.emit_count % 100000 == 0:
      self.logger.info("Emitted %s" % str(self.emit_count))
      self.logger.info("Emitting to error stream")
      self.emit(["test error message"], stream='error')
