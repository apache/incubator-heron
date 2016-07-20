# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Abstract class for Bolt/Spout -- Python interface of IInstance.java
# Tuple definition
import random
from collections import namedtuple

import time

StreamParseTuple = namedtuple('Tuple', 'id component stream task values')
"""Storm's primitive data type passed around via streams.

:ivar id: the ID of the Tuple (
:type id: str
:ivar component: component that the Tuple was generated from.
:type component: str
:ivar stream: the stream that the Tuple was emitted into.
:type stream: str
:ivar task: the task the Tuple was generated from.
:type task: int
:ivar values: the payload of the Tuple where data is stored.
:type values: tuple
"""

HeronTuple = namedtuple('Tuple', StreamParseTuple._fields + ('creation', ))
"""StreamParse compatible Heron Tuple

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
:ivar creation: the time the Tuple was created
:type creation: float
"""

RootTupleInfo = namedtuple('RootTupleInfo', 'stream_id tuple_id insertion_time key')


class TupleHelper(object):
  """Tuple generator, returns StreamParse compatible tuple"""
  TICK_TUPLE_ID = "__tick"
  TICK_SOURCE_COMPONENT = "__system"
  @staticmethod
  def make_tuple(stream, tuple_id, values, roots=None):
    component_name = stream.component_name
    stream_id = stream.id
    gen_task = roots[0].taskid if roots is not None else None
    return HeronTuple(id=str(tuple_id), component=component_name, stream=stream_id,
                      task=gen_task, values=values, creation=time.time())
  @staticmethod
  def make_tick_tuple():
    return HeronTuple(id=TupleHelper.TICK_TUPLE_ID, component=TupleHelper.TICK_SOURCE_COMPONENT,
                      stream=TupleHelper.TICK_TUPLE_ID, task=None, values=None,
                      creation=time.time())

  @staticmethod
  def make_root_tuple_info(stream_id, tuple_id):
    key = random.getrandbits(64)
    return RootTupleInfo(stream_id=stream_id, tuple_id=tuple_id, insertion_time=time.time(), key=key)


