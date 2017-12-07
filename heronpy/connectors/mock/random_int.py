# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''random_int.py: module for creating a simple random integer Generator'''

from random import randint
import time

from heronpy.streamlet.generator import Generator

class RandomIntGenerator(Generator):
  """A RandomIntGenerator generates an indefinite series of random numbers
  within user-specified bounds
  """

  def __init__(self, min_int, max_int, sleep=None):
    super(RandomIntGenerator, self).__init__()
    self._min = min_int
    self._max = max_int
    self._sleep = sleep

  def setup(self, context):
    pass
  
  def get(self):
    if self._sleep is not None:
      time.sleep(self._sleep)
    return randint(self._min_int, self._max_int)