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

'''test_runner.py: module for running the streamlet API in integration tests'''
from heronpy.streamlet.runner import Runner
from .test_topology_builder import TestTopologyBuilder

class TestRunner(Runner):
  """Module for running the streamlet API in integration tests"""
  def __init__(self):
    super().__init__()
    pass

  def run(self, name, config, builder, http_server_url):
    bldr = TestTopologyBuilder(name, http_server_url)
    builder.build(bldr)
    bldr.set_config(config._api_config)
    return bldr.create_topology()
