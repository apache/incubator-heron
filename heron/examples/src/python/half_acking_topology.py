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
'''Example HalfAckingTopology'''
import sys

from heron.pyheron.src.python import Grouping, TopologyBuilder, constants

from heron.examples.src.python.spout import WordSpout
from heron.examples.src.python.bolt import HalfAckBolt

# Topology is defined using a topology builder
# Refer to multi_stream_topology for defining a topology by subclassing Topology
if __name__ == '__main__':
  if len(sys.argv) != 2:
    print "Topology's name is not specified"
    sys.exit(1)

  builder = TopologyBuilder(name=sys.argv[1])

  word_spout = builder.add_spout("word_spout", WordSpout, par=2)
  half_ack_bolt = builder.add_bolt("half_ack_bolt", HalfAckBolt, par=2,
                                   inputs={word_spout: Grouping.fields('word')},
                                   config={constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS: 10})

  topology_config = {constants.TOPOLOGY_ENABLE_ACKING: True,
                     constants.TOPOLOGY_MAX_SPOUT_PENDING: 100000000,
                     constants.TOPOLOGY_MESSAGE_TIMEOUT_SECS: 300}

  builder.set_config(topology_config)

  builder.build_and_submit()
