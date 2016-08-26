# copyright 2016 twitter. all rights reserved.
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
'''half ack bolt'''
from heron.pyheron.src.python import Bolt

class HalfAckBolt(Bolt):
  """Half of data tuples will be acked and the other half will be failed"""
  # pylint: disable=unused-argument
  def initialize(self, config, context):
    self.total = 0

  def process(self, tup):
    self.total += 1
    if self.total % 2 == 0:
      self.logger.debug("Failing a tuple: %s" % str(tup))
      self.fail(tup)
    else:
      self.logger.debug("Acking a tuple: %s" % str(tup))
      self.ack(tup)

  def process_tick(self, tup):
    self.log("Got tick tuple!")
    self.log("Total received: %d" % self.total)
