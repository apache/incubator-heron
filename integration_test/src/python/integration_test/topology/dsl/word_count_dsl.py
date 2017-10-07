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
from integration_test.src.python.integration_test.common.generator import SleepArrayLooper

def word_count_dsl_builder(topology_name, http_server_url):
  builder = Builder()
  sentences = ["Mary had a little lamb",
               "Humpy Dumpy sat on a wall",
               "Here we round the Moulberry bush"]
  builder.new_source(SleepArrayLooper(sentences)) \
         .flat_map(lambda line: line.split()) \
         .map(lambda word: (word, 1)) \
         .reduce_by_key_and_window(WindowConfig.create_sliding_window(5, 5), lambda x, y: x + y) \
         .log()
  runner = TestRunner()
  config = Config()
  return runner.run(topology_name, config, builder, http_server_url)
