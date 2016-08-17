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
# pylint: disable=missing-docstring

from heron.pyheron.src.python import Grouping

from ...core import TestTopologyBuilder
from ...common.bolt import CountAggregatorBolt, WordCountBolt
from ...common.spout import ABSpout

def fields_grouping_builder(topology_name, http_server_url):
  builder = TestTopologyBuilder(topology_name, http_server_url)
  ab_spout = builder.add_spout("ab-spout", ABSpout, 1, max_executions=400)

  count_bolt = builder.add_bolt("count-bolt", WordCountBolt,
                                inputs={ab_spout: Grouping.fields('word')}, par=2)

  builder.add_bolt("sum-bolt", CountAggregatorBolt,
                   inputs={count_bolt: Grouping.NONE}, par=1)

  return builder.create_topology()
