#!/usr/bin/env python3
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

# pylint: disable=unused-argument
class CountBolt(Bolt):
  """CountBolt"""
  # output field declarer
  #outputs = ['word', 'count']

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
    self._increment(word, 10 if word == "heron" else 1)
    global_metrics.safe_incr('count')
    self.ack(tup)

  def process_tick(self, tup):
    self.log("Got tick tuple!")
    self.log("Current map: %s" % str(self.counter))
