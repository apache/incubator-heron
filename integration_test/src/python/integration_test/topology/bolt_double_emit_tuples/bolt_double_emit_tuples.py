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

# pylint: disable=missing-docstring

from heronpy.api.stream import Grouping

from integration_test.src.python.integration_test.core import TestTopologyBuilder
from integration_test.src.python.integration_test.common.bolt import DoubleTuplesBolt
from integration_test.src.python.integration_test.common.spout import ABSpout

def bolt_double_emit_tuples_builder(topology_name, http_server_url):
  builder = TestTopologyBuilder(topology_name, http_server_url)
  ab_spout = builder.add_spout("ab-spout", ABSpout, 1)

  builder.add_bolt("double-tuples-bolt", DoubleTuplesBolt,
                   inputs={ab_spout: Grouping.SHUFFLE},
                   par=1, optional_outputs=['word'])

  return builder.create_topology()
