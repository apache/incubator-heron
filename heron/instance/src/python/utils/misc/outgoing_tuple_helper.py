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

'''outgoing_tuple_helper.py: module to provide a helper class for preparing and pushing tuples'''
import sys

from heron.common.src.python.utils.log import Log
from heron.proto import tuple_pb2, topology_pb2, ckptmgr_pb2

import heron.instance.src.python.utils.system_constants as constants
from heron.instance.src.python.utils import system_config

# pylint: disable=too-many-instance-attributes
# pylint: disable=no-value-for-parameter
class OutgoingTupleHelper:
  """Helper class for preparing and pushing tuples to Out-Stream

  This is a Python implementation of OutgoingTupleCollection.java

  Handles basic methods for sending out tuples
  1. ``init_new_control_tuple()`` or ``init_new_data_tuple()``
  2. ``add_data_tuple()``, ``add_ack_tuple()`` and ``add_fail_tuple()``
  3. ``flush_remaining()`` tuples and send out the tuples

  :ivar out_stream: (HeronCommunicator) Out-Stream. Pushed message is an instance of HeronTupleSet
  :ivar pplan_helper: (PhysicalPlanHelper) Physical Plan Helper for this component
  :ivar current_data_tuple_set: (HeronDataTupleSet) currently buffered data tuple
  :ivar current_control_tuple_set: (HeronControlTupleSet) currently buffered control tuple
  """
  make_data_tuple_set = lambda _: tuple_pb2.HeronDataTupleSet()
  make_control_tuple_set = lambda _: tuple_pb2.HeronControlTupleSet()
  make_tuple_set = lambda _: tuple_pb2.HeronTupleSet()
  make_stream_id = lambda _: topology_pb2.StreamId()

  def __init__(self, pplan_helper, out_stream):
    self.out_stream = out_stream
    self.pplan_helper = pplan_helper

    self.current_data_tuple_set = None
    self.current_control_tuple_set = None

    self.current_data_tuple_size_in_bytes = 0
    self.total_data_emitted_in_bytes = 0

    self.sys_config = system_config.get_sys_config()

    # read the config values
    self.data_tuple_set_capacity = self.sys_config[constants.INSTANCE_SET_DATA_TUPLE_CAPACITY]
    self.max_data_tuple_size_in_bytes =\
      self.sys_config.get(constants.INSTANCE_SET_DATA_TUPLE_SIZE_BYTES, sys.maxsize)
    self.control_tuple_set_capacity = self.sys_config[constants.INSTANCE_SET_CONTROL_TUPLE_CAPACITY]

  def send_out_tuples(self):
    """Sends out currently buffered tuples into the Out-Stream"""
    self._flush_remaining()

  def add_data_tuple(self, stream_id, new_data_tuple, tuple_size_in_bytes):
    """Add a new data tuple to the currently buffered set of tuples"""
    if (self.current_data_tuple_set is None) or \
        (self.current_data_tuple_set.stream.id != stream_id) or \
        (len(self.current_data_tuple_set.tuples) >= self.data_tuple_set_capacity) or \
        (self.current_data_tuple_size_in_bytes >= self.max_data_tuple_size_in_bytes):
      self._init_new_data_tuple(stream_id)

    added_tuple = self.current_data_tuple_set.tuples.add()
    added_tuple.CopyFrom(new_data_tuple)

    self.current_data_tuple_size_in_bytes += tuple_size_in_bytes
    self.total_data_emitted_in_bytes += tuple_size_in_bytes

  def add_control_tuple(self, new_control_tuple, tuple_size_in_bytes, is_ack):
    """Add a new control (Ack/Fail) tuple to the currently buffered set of tuples

    :param is_ack: ``True`` if Ack, ``False`` if Fail
    """
    if self.current_control_tuple_set is None:
      self._init_new_control_tuple()
    elif is_ack and (len(self.current_control_tuple_set.fails) > 0 or
                     len(self.current_control_tuple_set.acks) >= self.control_tuple_set_capacity):
      self._init_new_control_tuple()
    elif not is_ack and \
        (len(self.current_control_tuple_set.acks) > 0 or
         len(self.current_control_tuple_set.fails) >= self.control_tuple_set_capacity):
      self._init_new_control_tuple()

    if is_ack:
      added_tuple = self.current_control_tuple_set.acks.add()
    else:
      added_tuple = self.current_control_tuple_set.fails.add()

    added_tuple.CopyFrom(new_control_tuple)

    self.total_data_emitted_in_bytes += tuple_size_in_bytes

  def add_ckpt_state(self, ckpt_id, ckpt_state):
    """Add the checkpoint state message to be sent back the stmgr

    :param ckpt_id: The id of the checkpoint
    :ckpt_state: The checkpoint state
    """
    # first flush any buffered tuples
    self._flush_remaining()
    msg = ckptmgr_pb2.StoreInstanceStateCheckpoint()
    istate = ckptmgr_pb2.InstanceStateCheckpoint()
    istate.checkpoint_id = ckpt_id
    istate.state = ckpt_state
    msg.state.CopyFrom(istate)
    self._push_tuple_to_stream(msg)

  def clear(self):
    """Clears current data and the streams"""
    self._flush_remaining()
    self.out_stream.clear()

  def _init_new_data_tuple(self, stream_id):
    self._flush_remaining()
    self.current_data_tuple_size_in_bytes = 0

    new_stream_id = self.make_stream_id()
    new_stream_id.id = stream_id
    new_stream_id.component_name = self.pplan_helper.my_component_name

    new_data_tuple_set = self.make_data_tuple_set()
    new_data_tuple_set.stream.CopyFrom(new_stream_id)
    self.current_data_tuple_set = new_data_tuple_set

  def _init_new_control_tuple(self):
    self._flush_remaining()
    self.current_control_tuple_set = self.make_control_tuple_set()

  def _flush_remaining(self):
    if self.current_data_tuple_set is not None:
      Log.debug("In flush_remaining() - flush data tuple set")
      tuple_set = self.make_tuple_set()
      tuple_set.data.CopyFrom(self.current_data_tuple_set)
      self._push_tuple_to_stream(tuple_set)
      self.current_data_tuple_set = None
      self.current_data_tuple_size_in_bytes = 0


    if self.current_control_tuple_set is not None:
      Log.debug("In flush_remaining() - flush control tuple set")
      tuple_set = self.make_tuple_set()
      tuple_set.control.CopyFrom(self.current_control_tuple_set)
      self._push_tuple_to_stream(tuple_set)
      self.current_control_tuple_set = None

  def _push_tuple_to_stream(self, tuple_set):
    self.out_stream.offer(tuple_set)

  def is_out_queue_available(self):
    return self.out_stream.get_available_capacity() > 0
