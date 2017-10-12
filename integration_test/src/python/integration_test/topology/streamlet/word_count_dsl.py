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
from heronpy.connectors.mock.arraylooper import ArrayLooper
from integration_test.src.python.integration_test.core.test_runner import TestRunner

def word_count_dsl_builder(topology_name, http_server_url):
  builder = Builder()
  sentences1 = ["Mary had a little lamb",
                "Humpy Dumpy sat on a wall"]
  sentences2 = ["Here we round the Moulberry bush"]
  source1 = builder.new_source(ArrayLooper(sentences1, sleep=1))
  source2 = builder.new_source(ArrayLooper(sentences2, sleep=1))
  stream1 = source1.flat_map(lambda line: line.split()) \
                   .map(lambda word: (word, 1))
  stream2 = source2.flat_map(lambda line: line.split()) \
                   .map(lambda word: (word, 1))
  stream1.union(stream2)
  runner = TestRunner()
  config = Config()
  return runner.run(topology_name, config, builder, http_server_url)
