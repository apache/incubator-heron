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
"""module for flatMap bolt: FlatMapBolt"""
import collections
from heron.api.src.python import Bolt, Stream

# pylint: disable=unused-argument
class FlatMapBolt(Bolt):
  """FlatMapBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  FUNCTION = 'function'

  def initialize(self, config, context):
    self.logger.debug("FlatMapBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if FlatMapBolt.FUNCTION in config:
      self.flatmap_function = config[FlatMapBolt.FUNCTION]
      if not callable(self.flatmap_function):
        raise RuntimeError("FlatMap function has to be callable")
    else:
      raise RuntimeError("FlatMapBolt needs to be passed flatMap function")

  def process(self, tup):
    retval = self.flatmap_function(tup.values[0])
    if isinstance(retval, collections.Iterable):
      for value in retval:
        self.emit([value], stream='output')
        self.emitted += 1
    else:
      self.emit([retval], stream='output')
      self.emitted += 1
    self.processed += 1
    self.ack(tup)
