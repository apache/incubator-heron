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

"""Word count bolt"""

from ...core import BatchBolt

class WordCountBolt(BatchBolt):
  """Word Count Bolt counts the number of different words received and emits that number"""
  outputs = ['count']

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    self.cache = {}

  def process(self, tup):
    word = tup.values[0]

    if word in self.cache:
      self.cache[word] += 1
    else:
      self.cache[word] = 1

    self.logger.info("Counter: %s", str(self.cache))

  def finish_batch(self):
    self.logger.info("In finish batch, emitting: %d", len(self.cache))
    self.emit([len(self.cache)])
