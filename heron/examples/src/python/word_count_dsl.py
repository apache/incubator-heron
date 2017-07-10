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

from heron.dsl.src.python import TimeWindow
from heron.spouts.src.python import FixedLinesStreamlet

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print "Topology's name is not specified"
    sys.exit(1)

  counts = FixedLinesStreamlet.fixedLinesGenerator(parallelism=2) \
           .flatMap(lambda line: line.split(), parallelism=2) \
           .map(lambda word: (word, 1), parallelism=2) \
           .reduceByWindow(TimeWindow(10, 2), lambda x, y: x + y)
  counts.run(sys.argv[1])
