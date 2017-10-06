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
'''Example WordCountTopology'''

import collections
import logging
import itertools
import time

from heronpy.dsl.builder import Builder
from heronpy.dsl.config import Config
from heronpy.dsl.generator import Generator
from heronpy.dsl.runner import Runner
from heronpy.dsl.windowconfig import WindowConfig
from integration_test.src.python.integration_test.core.test_runner import TestRunner

def word_count_dsl_builder(topology_name, http_server_url):
  builder = Builder()
  sentences = ["Mary had a little lamb",
               "Humpy Dumpy sat on a wall",
               "Here we round the Moulberry bush"]
  builder.new_source(ArrayLooper(sentences)) \
         .flat_map(lambda line: line.split()) \
         .map(lambda word: (word, 1)) \
         .reduce_by_key_and_window(WindowConfig.create_sliding_window(5, 5), lambda x, y: x + y) \
         .log()
  runner = TestRunner()
  config = Config()
  runner.run(topology_name, config, builder, http_server_url)

class ArrayLooper(Generator):
  """A ArrayLooper loops the contents of the a user supplied array forever
  """
  def __init__(self, user_iterable):
    super(ArrayLooper, self).__init__()
    if not isinstance(user_iterable, collections.Iterable):
      raise RuntimeError("ArrayLooper must be passed an iterable")
    self._user_iterable = user_iterable

  def setup(self, context):
    self._curiter = itertools.cycle(self._user_iterable)

  def get(self):
    time.sleep(1)
    return self._curiter.next()
