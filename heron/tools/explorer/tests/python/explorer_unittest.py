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

''' explorer_unittest.py '''
import json
import os
import unittest
from unittest.mock import Mock

from heron.tools.common.src.python.clients import tracker
from heron.tools.explorer.src.python import topologies


# pylint: disable=missing-docstring, no-self-use
class ExplorerTest(unittest.TestCase):
  ''' unit tests '''
  def sample_topo_result(self):
    info = []
    info.append(['a1', 'a2', 'a3'])
    info.append(['a1', 'a3', 'a5'])
    info.append(['a1', 'a3', 'a6'])
    info.append(['a2', 'a3', 'a6'])
    info.append(['b1', 'b2', 'b3'])
    info.append(['c1', 'c2', 'c3'])
    d = {}
    for row in info:
      tmp = d
      if row[0] not in tmp:
        tmp[row[0]] = {}
      tmp = tmp[row[0]]
      if row[1] not in tmp:
        tmp[row[1]] = []
      tmp[row[1]].append(row[2])
    return d, info

  def test_topo_result_to_table(self):
    d, told = self.sample_topo_result()
    tnew, _ = topologies.to_table(d)
    told.sort()
    tnew.sort()
    self.assertEqual(told, tnew)
