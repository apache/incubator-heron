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
"""module for filter bolt: FilterBolt"""
from heron.api.src.python import Bolt, Stream

# pylint: disable=unused-argument
class FilterBolt(Bolt):
  """FilterBolt"""
  # output declarer
  outputs = [Stream('output', ['_output_'])]
  FUNCTION = 'function'

  def initialize(self, config, context):
    self.logger.debug("In initialize() of FilterBolt")
    self.logger.debug("Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if FilterBolt.FUNCTION in config:
      self.filter_function = config[FilterBolt.FUNCTION]
      if not callable(self.filter_function):
        raise RuntimeError("Filter function has to be callable")
    else:
      raise RuntimeError("FilterBolt needs to be passed filter function")

  def process(self, tup):
    if self.filter_function(tup.values):
      self.emit([tup.values], stream='output')
      self.emitted += 1
    self.processed += 1
    self.ack(tup)
