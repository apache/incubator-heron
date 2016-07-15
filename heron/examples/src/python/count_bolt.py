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
from collections import Counter

from heron.instance.src.python.instance.bolt import Bolt
from heron.common.src.python.log import Log

class CountBolt(Bolt):
  outputs = ['word', 'count']

  def initialize(self, config, context):
    Log.debug("In prepare() of CountBolt")
    self.counter = Counter()
    self.total = 0

  def _increment(self, word, inc_by):
    self.counter[word] += inc_by
    self.total += inc_by

  def process(self, tuple):
    word = tuple.values[0]
    self._increment(word, 10 if word == "heron" else 1)
    if self.total % 1000 == 0:
      Log.info("Current map: " + str(self.counter))

    self.emit([word, self.counter[word]])

