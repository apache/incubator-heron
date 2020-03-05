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

"""module for example spout: WordSpout"""

from itertools import cycle
from collections import Counter
from heronpy.api.spout.spout import Spout
from heronpy.api.state.stateful_component import StatefulComponent

class StatefulWordSpout(Spout, StatefulComponent):
  """StatefulWordSpout: emits a set of words repeatedly"""
  # output field declarer
  outputs = ['word']

  # pylint: disable=attribute-defined-outside-init
  def init_state(self, stateful_state):
    self.recovered_state = stateful_state
    self.logger.info("Checkpoint Snapshot recovered : %s" % str(self.recovered_state))

  def pre_save(self, checkpoint_id):
    # Purely for debugging purposes
    for (k, v) in list(self.counter.items()):
      self.recovered_state.put(k, v)
    self.logger.info("Checkpoint Snapshot %s : %s" % (checkpoint_id, str(self.recovered_state)))

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    self.logger.info("In initialize() of WordSpout")
    self.words = cycle(["hello", "bye", "good", "bad", "heron", "storm"])
    self.counter = Counter()

    self.emit_count = 0
    self.ack_count = 0
    self.fail_count = 0

    self.logger.info("Component-specific config: \n%s" % str(config))

  def next_tuple(self):
    word = next(self.words)
    self.emit([word], tup_id='message id')
    self.counter[word] += 1
    self.emit_count += 1
    if self.emit_count % 100000 == 0:
      self.logger.info("Emitted " + str(self.emit_count))
