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

import logging
import time

from heronpy.dsl.builder import Builder
from heronpy.dsl.runner import Runner
from heronpy.dsl.config import Config
from heronpy.dsl.windowconfig import WindowConfig
from heronpy.connectors.mock.arraylooper import ArrayLooper

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
  runner = Runner()
  config = Config()
  runner.run(sys.argv[1], config, builder)

  counts = TestStreamlet(FixedLinesStreamlet.fixedLinesGenerator(parallelism=1) \
    .flat_map(lambda line: line.split(), parallelism=1) \
    .map(lambda word: (word, 1), parallelism=1) \
    .reduce_by_window(TimeWindow(5, 5), lambda x, y: x + y))
  return counts.run(topology_name, http_server_url=http_server_url)
