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

"""Base spout for integration tests"""
import copy
from heron.common.src.python.utils.log import Log
from heronpy.api.spout.spout import Spout
from heronpy.api.stream import Stream
from heronpy.api.component.component_spec import HeronComponentSpec
import heron.common.src.python.pex_loader as pex_loader

from ..core import constants as integ_const

class IntegrationTestSpout(Spout):
  """Base spout for integration test

  Every spout of integration test topology consists of this instance, each delegating user's spout.
  """
  outputs = [Stream(fields=[integ_const.INTEGRATION_TEST_TERMINAL],
                    name=integ_const.INTEGRATION_TEST_CONTROL_STREAM_ID)]

  @classmethod
  def spec(cls, name, par, config, user_spout_classpath, user_output_fields=None):
    python_class_path = f"{cls.__module__}.{cls.__name__}"

    config[integ_const.USER_SPOUT_CLASSPATH] = user_spout_classpath
    # avoid modification to cls.outputs
    _outputs = copy.copy(cls.outputs)
    if user_output_fields is not None:
      _outputs.extend(user_output_fields)
    return HeronComponentSpec(name, python_class_path, is_spout=True, par=par,
                              inputs=None, outputs=_outputs, config=config)

  def initialize(self, config, context):
    user_spout_classpath = config.get(integ_const.USER_SPOUT_CLASSPATH, None)
    if user_spout_classpath is None:
      raise RuntimeError("User defined integration test spout was not found")
    user_spout_cls = self._load_user_spout(context.get_topology_pex_path(), user_spout_classpath)
    self.user_spout = user_spout_cls(delegate=self)

    self.max_executions = config.get(integ_const.USER_MAX_EXECUTIONS, integ_const.MAX_EXECUTIONS)
    assert isinstance(self.max_executions, int) and self.max_executions > 0
    Log.info("Max executions: %d", self.max_executions)
    self.tuples_to_complete = 0

    self.user_spout.initialize(config, context)

  @staticmethod
  def _load_user_spout(pex_file, classpath):
    pex_loader.load_pex(pex_file)
    cls = pex_loader.import_and_get_class(pex_file, classpath)
    return cls

  @property
  def is_done(self):
    return self.max_executions == 0

  def next_tuple(self):
    if self.is_done:
      return

    self.max_executions -= 1
    Log.info("max executions: %d", self.max_executions)

    self.user_spout.next_tuple()

    if self.is_done:
      self._emit_terminal_if_needed()
      Log.info("This topology is finished.")

  def ack(self, tup_id):
    Log.info("Received an ack with tuple id: %s", str(tup_id))
    self.tuples_to_complete -= 1
    if tup_id != integ_const.INTEGRATION_TEST_MOCK_MESSAGE_ID:
      self.user_spout.ack(tup_id)
    self._emit_terminal_if_needed()

  def fail(self, tup_id):
    Log.info("Received a fail message with tuple id: %s", str(tup_id))
    self.tuples_to_complete -= 1
    if tup_id != integ_const.INTEGRATION_TEST_MOCK_MESSAGE_ID:
      self.user_spout.fail(tup_id)
    self._emit_terminal_if_needed()

  def emit(self, tup, tup_id=None, stream=Stream.DEFAULT_STREAM_ID,
           direct_task=None, need_task_ids=None):
    """Emits from this integration test spout

    Overriden method which will be called when user's spout calls emit()
    """
    # if is_control True -> control stream should not count
    self.tuples_to_complete += 1

    if tup_id is None:
      Log.info("Add tup_id for tuple: %s", str(tup))
      _tup_id = integ_const.INTEGRATION_TEST_MOCK_MESSAGE_ID
    else:
      _tup_id = tup_id

    super().emit(tup, _tup_id, stream, direct_task, need_task_ids)

  def _emit_terminal_if_needed(self):
    Log.info("is_done: %s, tuples_to_complete: %s", self.is_done, self.tuples_to_complete)
    if self.is_done and self.tuples_to_complete == 0:
      Log.info("Emitting terminals to downstream")
      super().emit([integ_const.INTEGRATION_TEST_TERMINAL],
                                             stream=integ_const.INTEGRATION_TEST_CONTROL_STREAM_ID)
