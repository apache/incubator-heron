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
"""module for example bolt: WindowSizeBolt"""
from heron.api.src.python import SlidingWindowBolt

# pylint: disable=unused-argument
class WindowSizeBolt(SlidingWindowBolt):
  """WindowSizeBolt
     A bolt that calculates the average batch size of window"""

  def initialize(self, config, context):
    super(WindowSizeBolt, self).initialize(config, context)
    self.numerator = 0.0
    self.denominator = 0.0

  def processWindow(self, window_info, tuples):
    self.numerator += len(tuples)
    self.denominator += 1
    self.logger.info("The current average is %f" % (self.numerator / self.denominator))
