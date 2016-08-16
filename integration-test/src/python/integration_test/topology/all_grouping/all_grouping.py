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
"""All grouping topology"""

from heron.pyheron.src.python import Grouping

from ...core import TestTopologyBuilder
from ...common.bolt import IdentityBolt
from ...common.spout import ABSpout

def all_grouping_buidler(topology_name, http_server_url):
  """Integration test topology builder for all grouping"""
  builder = TestTopologyBuilder(topology_name, http_server_url)
  ab_spout = builder.add_spout("ab-spout", ABSpout, 1)

  builder.add_bolt("identity-bolt", IdentityBolt,
                   inputs={ab_spout: Grouping.ALL},
                   par=2,
                   optional_outputs=['word'])

  return builder.create_topology()
