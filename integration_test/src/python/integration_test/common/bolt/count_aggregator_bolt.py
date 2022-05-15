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

"""Count aggregator bolt"""

from ...core import BatchBolt

class CountAggregatorBolt(BatchBolt):
  """Bolt to count how many different words received"""
  outputs = ['sum']

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    self.sum = 0

  def process(self, tup):
    self.sum += int(tup.values[0])

  def finish_batch(self):
    self.logger.info("In finish batch, emitting: %d", self.sum)
    self.emit([self.sum])
