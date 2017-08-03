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
"""module for example bolt: CountBolt"""
from collections import Counter
from heron.api.src.python import global_metrics
from heron.api.src.python import Bolt, StatefulComponent

# pylint: disable=unused-argument
class StatefulCountBolt(Bolt, StatefulComponent):
  """CountBolt"""
  # output field declarer
  #outputs = ['word', 'count']

  # pylint: disable=attribute-defined-outside-init
  def initState(self, stateful_state):
    self.recovered_state = stateful_state
    self.logger.info("Recovered state")
    self.logger.info(str(self.recovered_state))

  def preSave(self, checkpoint_id):
    self.logger.info("PreSave of %s" % checkpoint_id)
    for (k, v) in self.counter.items():
      self.recovered_state.put(k, v)
    self.logger.info(str(self.recovered_state))

  def initialize(self, config, context):
    self.logger.info("In prepare() of CountBolt")
    self.counter = Counter()
    self.total = 0

    self.logger.info("Component-specific config: \n%s" % str(config))
    self.logger.info("Context: \n%s" % str(context))

  def _increment(self, word, inc_by):
    self.counter[word] += inc_by
    self.total += inc_by

  def process(self, tup):
    word = tup.values[0]
    self._increment(word, 1)
    global_metrics.safe_incr('count')
    self.ack(tup)

  def process_tick(self, tup):
    self.log("Got tick tuple!")
    self.log("Current map: %s" % str(self.counter))
