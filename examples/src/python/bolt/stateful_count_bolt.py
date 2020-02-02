#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""module for example bolt: CountBolt"""
from collections import Counter

import heronpy.api.global_metrics as global_metrics
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent

# pylint: disable=unused-argument
class StatefulCountBolt(Bolt, StatefulComponent):
  """CountBolt"""
  # output field declarer
  #outputs = ['word', 'count']

  # pylint: disable=attribute-defined-outside-init
  def init_state(self, stateful_state):
    self.recovered_state = stateful_state
    self.logger.info("Checkpoint Snapshot recovered : %s" % str(self.recovered_state))

  def pre_save(self, checkpoint_id):
    for (k, v) in list(self.counter.items()):
      self.recovered_state.put(k, v)
    self.logger.info("Checkpoint Snapshot %s : %s" % (checkpoint_id, str(self.recovered_state)))

  def initialize(self, config, context):
    self.logger.info("In prepare() of CountBolt")
    self.counter = Counter()
    self.total = 0

    self.logger.info("Component-specific config: \n%s" % str(config))

  def _increment(self, word, inc_by):
    self.counter[word] += inc_by
    self.total += inc_by

  def process(self, tup):
    word = tup.values[0]
    self._increment(word, 1)
    global_metrics.safe_incr('count')
    self.ack(tup)

  def process_tick(self, tup):
    pass
