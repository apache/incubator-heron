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

"""Base bolt for integration tests"""
import copy

from heron.common.src.python.utils.log import Log
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.stream import Stream
from heronpy.api.component.component_spec import HeronComponentSpec
import heron.common.src.python.pex_loader as pex_loader

from ..core import constants as integ_const
from .batch_bolt import BatchBolt

# pylint: disable=missing-docstring
class IntegrationTestBolt(Bolt):
  """Base bolt for integration test

  Every bolt of integration test topology consists of this instance, each delegating user's bolt.
  """
  outputs = [Stream(fields=[integ_const.INTEGRATION_TEST_TERMINAL],
                    name=integ_const.INTEGRATION_TEST_CONTROL_STREAM_ID)]

  @classmethod
  def spec(cls, name, par, inputs, config, user_bolt_classpath, user_output_fields=None):
    python_class_path = "%s.%s" % (cls.__module__, cls.__name__)
    config[integ_const.USER_BOLT_CLASSPATH] = user_bolt_classpath
    # avoid modification to cls.outputs
    _outputs = copy.copy(cls.outputs)
    if user_output_fields is not None:
      _outputs.extend(user_output_fields)
    return HeronComponentSpec(name, python_class_path, is_spout=False, par=par,
                              inputs=inputs, outputs=_outputs, config=config)

  def initialize(self, config, context):
    user_bolt_classpath = config.get(integ_const.USER_BOLT_CLASSPATH, None)
    if user_bolt_classpath is None:
      raise RuntimeError("User defined integration bolt was not found")
    user_bolt_cls = self._load_user_bolt(context.get_topology_pex_path(), user_bolt_classpath)
    self.user_bolt = user_bolt_cls(delegate=self)

    upstream_components = set()
    self.terminal_to_receive = 0
    for streamId in list(context.get_this_sources().keys()):
      # streamId is topology_pb2.StreamId protobuf message
      upstream_components.add(streamId.component_name)
    for comp_name in upstream_components:
      self.terminal_to_receive += len(context.get_component_tasks(comp_name))

    self.tuple_received = 0
    self.tuples_processed = 0
    self.current_tuple_processing = None

    Log.info("Terminals to receive: %d" % self.terminal_to_receive)
    self.user_bolt.initialize(config, context)

  @staticmethod
  def _load_user_bolt(pex_file, classpath):
    pex_loader.load_pex(pex_file)
    cls = pex_loader.import_and_get_class(pex_file, classpath)
    return cls

  @property
  def is_done(self):
    return self.terminal_to_receive == 0

  def process(self, tup):
    self.tuple_received += 1
    stream_id = tup.stream

    Log.info("Received a tuple: %s from %s" % (tup, stream_id))
    if stream_id == integ_const.INTEGRATION_TEST_CONTROL_STREAM_ID:
      self.terminal_to_receive -= 1
      if self.is_done:
        if isinstance(self.user_bolt, BatchBolt):
          Log.info("Invoke bolt to do finish batch")
          self.user_bolt.finish_batch()

        Log.info("Populating the terminals to downstream")
        super(IntegrationTestBolt, self).emit(
            [integ_const.INTEGRATION_TEST_TERMINAL],
            stream=integ_const.INTEGRATION_TEST_CONTROL_STREAM_ID)
    else:
      self.current_tuple_processing = tup
      self.user_bolt.process(tup)
      self.ack(tup)

  def emit(self, tup, stream=Stream.DEFAULT_STREAM_ID, anchors=None,
           direct_task=None, need_task_ids=False):
    Log.info("emitting tuple: %s", tup)
    if tup is None:
      super(IntegrationTestBolt, self).emit(list(self.current_tuple_processing),
                                            stream=stream, anchors=anchors,
                                            direct_task=direct_task, need_task_ids=need_task_ids)
    else:
      super(IntegrationTestBolt, self).emit(tup, stream, anchors, direct_task, need_task_ids)

  def ack(self, tup):
    Log.info("Trying to do an ack. tuples processed: %d, received: %d"
             % (self.tuples_processed, self.tuple_received))
    if self.tuples_processed < self.tuple_received:
      super(IntegrationTestBolt, self).ack(tup)
      self.tuples_processed += 1

  def fail(self, tup):
    Log.info("Trying to do a fail. tuples processed: %d, received: %d"
             % (self.tuples_processed, self.tuple_received))
    if self.tuples_processed < self.tuple_received:
      super(IntegrationTestBolt, self).fail(tup)
      self.tuples_processed += 1

  def process_tick(self, tup):
    self.user_bolt.process_tick(tup)
