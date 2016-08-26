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
'''module for example topology: CustomGroupingTopology'''

from heron.common.src.python.utils.log import Log
from heron.pyheron.src.python import Topology, Grouping, ICustomGrouping, constants

from heron.examples.src.python.spout import WordSpout
from heron.examples.src.python.bolt import ConsumeBolt

# pylint: disable=unused-argument
class SampleCustomGrouping(ICustomGrouping):
  def prepare(self, context, component, stream, target_tasks):
    Log.info("In prepare of SampleCustomGrouping, "
             "with src component: %s, "
             "with stream id: %s, "
             "with target tasks: %s"
             % (component, stream, str(target_tasks)))
    self.target_tasks = target_tasks

  def choose_tasks(self, values):
    # only emits to the first task id
    return [self.target_tasks[0]]

class CustomGrouping(Topology):
  custom_grouping_path = "heron.examples.src.python.custom_grouping_topology.SampleCustomGrouping"

  word_spout = WordSpout.spec(par=1)
  consume_bolt = ConsumeBolt.spec(par=3,
                                  inputs={word_spout: Grouping.custom(custom_grouping_path)},
                                  config={constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS: 10})
