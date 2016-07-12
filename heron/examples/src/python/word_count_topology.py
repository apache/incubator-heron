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
from heron.examples.src.python.word_spout import WordSpout
from heron.examples.src.python.count_bolt import CountBolt
import sys
import os

def usage():
  print sys.argv[0] + " <topology_tmp_dir>"
  print "  <topology_tmp_dir> is a directory in which topology.defn file will be created"
  sys.exit(1)

if __name__ == '__main__':
  if len(sys.argv) != 2:
    usage()

  print ("DEBUG: In main of WordCountTopology")

  word_count_topology = Topology("WordCountTopology")
  word_count_topology.set_spout("word_spout", WordSpout, "heron.examples.src.python.word_spout.WordSpout")
  word_count_topology.set_bolt("count_bolt", CountBolt, "heron.examples.src.python.count_bolt.CountBolt", inputs=["word_spout"])

  word_count_topology.write_to_file(sys.argv[1])

  print("DEBUG: Wrote topology definition")

  path = os.path.join(sys.argv[1], "topo_info")
  info = "Topology id: " + word_count_topology.topology_id + "\n"
  with open(path, 'w') as f:
    f.write(info)


