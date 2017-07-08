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
"""module for sample bolt: SampleBolt
   SampleBolt is a more sophisticated FilterBolt which
   can do sampling of the data that it recieves and emit
   only sampled tuples"""
from heron.api.src.python import Bolt, Stream

# pylint: disable=unused-argument
class SampleBolt(Bolt):
  """SampleBolt"""
  # output declarer
  outputs = [Stream(fields=['_output_'], name='output')]
  FRACTION = 'fraction'

  def initialize(self, config, context):
    self.logger.debug("SampleBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if SampleBolt.FRACTION in config:
      self.sample_fraction = config[SampleBolt.FRACTION]
      if not isinstance(self.sample_fraction, float):
        raise RuntimeError("Sample fraction has to be a float")
      if self.sample_fraction > 1.0:
        raise RuntimeError("Sample fraction has to be <= 1.0")
    else:
      raise RuntimeError("SampleBolt needs to be passed filter function")

  def process(self, tup):
    self.processed += 1
    self.ack(tup)
    raise RuntimeError("SampleBolt not fully functional")
