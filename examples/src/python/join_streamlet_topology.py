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

'''join_streamlet_topology.py: module is an example of how to use the join operator'''

import sys

from heronpy.streamlet.builder import Builder
from heronpy.streamlet.runner import Runner
from heronpy.streamlet.config import Config
from heronpy.streamlet.windowconfig import WindowConfig
from heronpy.connectors.mock.arraylooper import ArrayLooper

# pylint: disable=superfluous-parens
if __name__ == '__main__':
  if len(sys.argv) != 2:
    print("Topology's name is not specified")
    sys.exit(1)

  builder = Builder()

  source_1 = builder.new_source(ArrayLooper([["key1", "a"], ["key1", "b"]], sleep=1))

  source_2 = builder.new_source(ArrayLooper([["key1", "c"], ["key1", "d"]], sleep=1))

  source_1.join(source_2, WindowConfig.create_sliding_window(2, 1), lambda x, y: x + y).log()

  runner = Runner()
  config = Config()
  runner.run(sys.argv[1], config, builder)
