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

from heronpy.api.spout.spout import Spout

class WordSpout(Spout):
  """WordSpout: emits a set of words repeatedly"""
  # output field declarer
  outputs = ['word']

  def initialize(self, config, context):
    self.logger.info("In initialize() of WordSpout")
    self.words = cycle(["hello", "bye", "good", "bad", "heron", "storm"])

    self.emit_count = 0
    self.ack_count = 0
    self.fail_count = 0

    self.logger.info("Component-specific config: \n%s" % str(config))
    self.logger.info("Context: \n%s" % str(context))

  def next_tuple(self):
    word = next(self.words)
    self.emit([word], tup_id='message id')
    self.emit_count += 1
    if self.emit_count % 100000 == 0:
      self.logger.info("Emitted " + str(self.emit_count))

  def ack(self, tup_id):
    self.ack_count += 1
    if self.ack_count % 100000 == 0:
      self.logger.info("Acked %sth tuples, tup_id: %s" % (str(self.ack_count), str(tup_id)))

  def fail(self, tup_id):
    self.fail_count += 1
    if self.fail_count % 100000 == 0:
      self.logger.info("Failed %sth tuples, tup_id: %s" % (str(self.fail_count), str(tup_id)))
