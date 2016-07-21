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

from heron.instance.src.python.instance.topology import Topology
from heron.instance.src.python.instance.stream import Grouping
from heron.examples.src.python.word_spout import WordSpout
from heron.examples.src.python.count_bolt import CountBolt

class WordCount(Topology):
  word_spout = WordSpout.spec(par=1)
  count_bolt = CountBolt.spec(par=1, inputs={word_spout: Grouping.fields('word')})
