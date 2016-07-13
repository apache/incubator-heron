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
from heron.common.src.python.color import Log

class CountBolt(Bolt):
  def __init__(self, pplan_helper, in_stream, out_stream):
    super(CountBolt, self).__init__(pplan_helper, in_stream, out_stream)
    self.counter = Counter()
    self.total = 0
    self.tuple_count = 0

  def prepare(self, config, context):
    Log.debug("In prepare() of CountBolt")
    pass

  def _increment(self, word, inc_by):
    self.counter[word] += inc_by
    self.total += inc_by

  def execute(self, tuple):
    word = tuple[0]
    self.tuple_count += 1
    Log.debug("Tuple count: " + str(self.tuple_count))
    self._increment(word, 10 if word == "heron" else 1)
    if self.total % 1000 == 0:
      Log.info("Current map: " + str(self.counter))

