#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

'''module for example topology: CustomGroupingTopology'''

import heronpy.api.api_constants as constants
from heronpy.api.topology import Topology
from heronpy.api.stream import Grouping

from examples.src.python.spout import MultiStreamSpout
from examples.src.python.bolt import CountBolt, StreamAggregateBolt

class MultiStream(Topology):
  spout = MultiStreamSpout.spec(par=2)
  count_bolt = CountBolt.spec(par=2,
                              inputs={spout: Grouping.fields('word')},
                              config={constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS: 10})
  stream_aggregator = StreamAggregateBolt.spec(par=1,
                                               inputs={spout: Grouping.ALL,
                                                       spout['error']: Grouping.ALL},
                                               config={constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS: 15})
