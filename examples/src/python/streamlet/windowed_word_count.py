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
'''Example windowed word count Streamlet API topology'''
import sys

from heronpy.streamlet.builder import Builder
from heronpy.streamlet.runner import Runner
from heronpy.streamlet.config import Config
from heronpy.streamlet.windowconfig import WindowConfig
from heronpy.connectors.mock.arraylooper import ArrayLooper

SENTENCES = [
  "I have nothing to declare but my genius",
  "You can even",
  "Compassion is an action word with no boundaries",
  "To thine own self be true"
]

def log_result(result):
  msg = "(word: %s, count: %d)".format(kv.get_key().get_key(), kv.get_value())
  print(msg)

# pylint: disable=superfluous-parens
if __name__ == '__main__':
  if len(sys.argv) != 2:
    print("Topology's name is not specified")
    sys.exit(1)
  
  if len(sys.argv) > 2:
    container_parallelism = sys.argv[2]
  else:
    container_parallelism = 2

  graph_builder = Builder(source=ArrayLooper(SENTENCES)) \
    .set_name("random-sentences-source")

  graph_builder.flat_map(lambda line: line.split()) \
               .set_name("flatten-into-individual-words") \
               .reduce_by_key_and_window(WindowConfig.create_sliding_window(10, 2), lambda x, y: x + y) \
               .set_name("reduce-operation") \
               .consume(lambda result: log_result(result))

  runner = Runner()
  config = Config(num_containers=container_parallelism)
  runner.run(sys.argv[1], config, graph_builder)
