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
import sys

from heronpy.dsl.builder import Builder
from heronpy.dsl.runner import Runner
from heronpy.dsl.config import Config
from heronpy.dsl.windowconfig import WindowConfig
from heronpy.connectors.mock.arraylooper import ArrayLooper

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print "Topology's name is not specified"
    sys.exit(1)

  builder = Builder()
  builder.new_source(ArrayLooper(["Mary Had a little lamb", "I Love You"])) \
         .flat_map(lambda line: line.split()) \
         .map(lambda word: (word, 1)) \
         .reduce_by_key_and_window(WindowConfig.create_sliding_window(10, 2), lambda x, y: x + y) \
         .log()
  runner = Runner()
  config = Config()
  runner.run(sys.argv[1], config, builder)
