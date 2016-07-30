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
from heron.instance.src.python.instance.stream import Grouping, GlobalStreamId
from heron.examples.src.python.word_spout import WordSpout
from heron.examples.src.python.count_bolt import CountBolt

import heron.common.src.python.constants as constants

class WordCount(Topology):
  task_hooks = ["heron.examples.src.python.test_task_hook.TestTaskHook"]
  config = {constants.TOPOLOGY_ENABLE_ACKING: "true",
            constants.TOPOLOGY_MAX_SPOUT_PENDING: 100000000,
            constants.TOPOLOGY_AUTO_TASK_HOOKS: task_hooks,
            "topology.wide.config.sample": {"key1": 12, "key2": 34}}

  #another_stream = GlobalStreamId(componentId="word_spout", streamId="another_stream")
  word_spout = WordSpout.spec(par=1)
  #count_bolt = CountBolt.spec(par=1, inputs={word_spout: Grouping.fields('word'),
  #                                           another_stream: Grouping.fields('word')})
  count_bolt = CountBolt.spec(par=1,
                              inputs={word_spout: Grouping.fields('word')},
                              config={constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS: 10,
                                      "count_bolt.specific": ["123", (12, 34)]})
