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

from itertools import cycle
from heron.instance.src.python.instance.spout import Spout


class WordSpout(Spout):
  outputs = ['word']

  def initialize(self, config, context):
    self.logger.info("In initialize() of WordSpout")
    self.words = cycle(["hello", "bye", "good", "bad", "heron", "storm"])
    self.emit_count = 0
    self.logger.debug("Spout context: \n" + str(context))

  def next_tuple(self):
    word = next(self.words)
    self.logger.debug("Will emit: " + word)
    self.emit_count += 1
    self.emit([word])
    self.logger.debug("Emit count: " + str(self.emit_count))
    if self.emit_count % 1000 == 0:
      self.logger.info("Emitted " + str(self.emit_count))
