#!/usr/bin/env python
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

'''state manager factory unittest'''
import unittest2 as unittest

from heron.statemgrs.src.python.config import Config
from heron.statemgrs.src.python import statemanagerfactory

class StateManagerFacotryTest(unittest.TestCase):
  """Unittest for statemanagerfactory"""

  def test_all_zk_supports_comma_separated_hostports(self):
    """Verify that a comma separated list of host ports is ok"""
    conf = Config()
    conf.set_state_locations([{'type':'zookeeper', 'name':'zk', 'hostport':'127.0.0.1:2181,127.0.0.1:2281',
                              'rootpath':'/heron', 'tunnelhost':'127.0.0.1'}])
    statemanagers = statemanagerfactory.get_all_zk_state_managers(conf)
    # 1 state_location should result in 1 state manager
    self.assertEqual(1, len(statemanagers))

    statemanager = statemanagers[0]
    # statemanager.hostportlist should contain both host port pairs
    self.assertTrue(('127.0.0.1', 2181) in statemanager.hostportlist)
    self.assertTrue(('127.0.0.1', 2281) in statemanager.hostportlist)
