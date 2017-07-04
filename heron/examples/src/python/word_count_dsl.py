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

from heron.dsl.src.python import StaticLinesStreamlet

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print "Topology's name is not specified"
    sys.exit(1)

  lines = StaticLinesStreamlet.staticLinesGenerator(stage_name=sys.argv[1], parallelism=2)
  words = lines.flatMap(lambda line: line.split(), parallelism=2)
  wordcounts = words.map(lambda word: (word, 1), parallelism=2)
  counts = wordcounts.reduceByWindow(lambda x, y: x + y)
  counts.run()
