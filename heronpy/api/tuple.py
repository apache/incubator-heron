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
'''tuple.py: heron's default data type'''

from collections import namedtuple

Tuple = namedtuple('Tuple', 'id component stream task values')
"""Storm's primitive data type passed around via streams.

:ivar id: the ID of the Tuple
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

class TupleHelper(object):
  """Tuple generator, returns StreamParse compatible tuple"""
  TICK_TUPLE_ID = "__tick"
  TICK_SOURCE_COMPONENT = "__system"
