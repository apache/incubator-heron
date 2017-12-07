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
'''Example filesystem sink Streamlet API topology'''
from random import randint
import sys

from heronpy.streamlet.builder import Builder
from heronpy.streamlet.runner import Runner
from heronpy.connectors.mock.randomint import RandomIntGenerator

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print("Topology's name is not specified")
    sys.exit(1)

  if len(sys.argv) > 2:
    container_parallelism = sys.argv[2]
  else:
    container_parallelism = 2

  graph_builder = Builder(source=RandomIntGenerator(1, 100, sleep=0.5)) \
    .set_name("incoming-integers")
  
  graph_builder.to_sink(FilesystemSink())

  runner = Runner()
  config = Config(num_containers=container_parallelism)
  runner.run(sys.argv[1], config, graph_builder)