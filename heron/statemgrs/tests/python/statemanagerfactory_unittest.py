# Copyright 2016 Twitter. All rights reserved.
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
'''state manager factory unittest'''
import unittest2 as unittest

from heron.statemgrs.src.python.config import Config
from heron.statemgrs.src.python import statemanagerfactory

class StateManagerFacotryTest(unittest.TestCase):
  """Unittest for statemanagerfactory"""

  def test_all_zk_supports_comma_separated_hostports(self):
    """Verify that a comma separated list of host ports is ok"""
    conf = Config()
    conf.set_state_locations([{'type':'zookeeper', 'name':'zk', 'hostport':'localhost:2181,localhost:2281',
                              'rootpath':'/heron', 'tunnelhost':'localhost'}])
    statemanagers = statemanagerfactory.get_all_zk_state_managers(conf)
    # 1 state_location should result in 1 state manager
    self.assertEquals(1, len(statemanagers))

    statemanager = statemanagers[0]
    # statemanager.hostportlist should contain both host port pairs
    self.assertTrue(('localhost', 2181) in statemanager.hostportlist)
    self.assertTrue(('localhost', 2281) in statemanager.hostportlist)