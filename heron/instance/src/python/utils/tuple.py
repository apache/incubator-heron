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

'''tuple.py: heron's default data type'''

import time
import random

from collections import namedtuple

from heronpy.api.tuple import Tuple

HeronTuple = namedtuple('Tuple', Tuple._fields + ('creation_time', 'roots'))
"""Internal manifestation of the Heron Tuple

:ivar id: the ID of the Tuple
:type id: str
:ivar component: component that the Tuple was generated from.
:type component: str
:ivar stream: the stream that the Tuple was emitted into.
:type stream: str
:ivar task: the task the Tuple was generated from.
:type task: int
:ivar values: the payload of the Tuple where data is stored.
:type values: tuple or list
:ivar creation_time: the time the Tuple was created
:type creation_time: float
:ivar roots: a list of RootId (protobuf)
:type roots: list
"""

class RootTupleInfo(namedtuple('RootTupleInfo', 'stream_id tuple_id insertion_time key')):
  __slots__ = ()
  def is_expired(self, current_time, timeout_sec):
    return self.insertion_time + timeout_sec - current_time <= 0

class TupleHelper(object):
  """Tuple Helper, returns Heron Tuple compatible tuple"""
  TICK_TUPLE_ID = "__tick"
  TICK_SOURCE_COMPONENT = "__system"

  #last three bits are used for type
  MAX_SFIXED64_RAND_BITS = 61

  @staticmethod
  def make_tuple(stream, tuple_key, values, roots=None):
    """Creates a HeronTuple

    :param stream: protobuf message ``StreamId``
    :param tuple_key: tuple id
    :param values: a list of values
    :param roots: a list of protobuf message ``RootId``
    """
    component_name = stream.component_name
    stream_id = stream.id
    gen_task = roots[0].taskid if roots is not None and len(roots) > 0 else None
    return HeronTuple(id=str(tuple_key), component=component_name, stream=stream_id,
                      task=gen_task, values=values, creation_time=time.time(), roots=roots)
  @staticmethod
  def make_tick_tuple():
    """Creates a TickTuple"""
    return HeronTuple(id=TupleHelper.TICK_TUPLE_ID, component=TupleHelper.TICK_SOURCE_COMPONENT,
                      stream=TupleHelper.TICK_TUPLE_ID, task=None, values=None,
                      creation_time=time.time(), roots=None)

  @staticmethod
  def make_root_tuple_info(stream_id, tuple_id):
    """Creates a RootTupleInfo"""
    key = random.getrandbits(TupleHelper.MAX_SFIXED64_RAND_BITS)
    return RootTupleInfo(stream_id=stream_id, tuple_id=tuple_id,
                         insertion_time=time.time(), key=key)
