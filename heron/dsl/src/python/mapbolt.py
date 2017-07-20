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
"""module for map bolt: MapBolt"""
from heron.api.src.python import Bolt, Stream

# pylint: disable=unused-argument
class MapBolt(Bolt):
  """MapBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  FUNCTION = 'function'

  def initialize(self, config, context):
    self.logger.debug("MapBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if MapBolt.FUNCTION in config:
      self.map_function = config[MapBolt.FUNCTION]
      if not callable(self.map_function):
        raise RuntimeError("Map function has to be callable")
    else:
      raise RuntimeError("MapBolt needs to be passed map function")

  def process(self, tup):
    retval = self.map_function(tup.values[0])
    self.emit([retval], stream='output')
    self.processed += 1
    self.emitted += 1
    self.ack(tup)
