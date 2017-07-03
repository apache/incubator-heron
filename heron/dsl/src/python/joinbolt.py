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
"""module for join bolt: JoinBolt"""
from heron.api.src.python import TumbingWindowBolt, Stream
from heron.api.src.python.custom_grouping import ICustomGrouping

# pylint: disable=unused-argument
class JoinBolt(TumbingWindowBolt):
  """JoinBolt"""
  # output declarer
  outputs = [Stream('output', ['_output_'])]
  TIMEWINDOW = TumbingWindowBolt.WINDOW_DURATION_SECS

  @staticmethod
  def _add(key, value, mymap):
    if key in mymap:
      mymap[key].append(value)
    else:
      mymap[key] = [value]

  def processWindow(self, window_config, tuples):
    # our temporary map
    mymap = {}
    for tup in tuples:
      userdata = tup.values[0]
      if not isinstance(userdata, list) or len(userdata) != 2:
        raise RuntimeError("Join tuples must be of list type of length 2")
      self._add(userdata[0], userdata[1], mymap)
      self.ack(tup)
    for (key, values) in mymap.items():
      self.emit([key, values], stream='output')

# pylint: disable=unused-argument
class JoinGrouping(ICustomGrouping):
  def prepare(self, context, component, stream, target_tasks):
    self.target_tasks = target_tasks

  def choose_tasks(self, values):
    if not isinstance(values, list) or len(values) != 2:
      raise RuntimeError("Tuples going to map must be of list type of length 2")
    # only emits to the first task id
    hashvalue = hash(values[0])
    target_index = hashvalue % len(self.target_tasks)
    return [self.target_tasks[target_index]]
