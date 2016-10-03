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
'''ZkStateManager unittest'''
import unittest2 as unittest

from heron.statemgrs.src.python.zkstatemanager import ZkStateManager


class ZkStateManagerTest(unittest.TestCase):
  """Unittest for ZkStateManager"""

  class MockKazooClient:
    def __init__(self):
      self.start_calls = 0
      self.stop_calls = 0

    def start(self):
      self.start_calls = self.start_calls + 1

    def stop(self):
      self.stop_calls = self.stop_calls + 1

    def add_listener(self,listener):
      pass

  def setUp(self):
    # Create a a ZkStateManager that we will test with
    self.statemanager = ZkStateManager('zk', [('localhost', 2181), ('localhost', 2281)], 'heron', 'reachable.host')
    # replace creation of a KazooClient
    self.mock_kazoo = ZkStateManagerTest.MockKazooClient()
    self.opened_host_ports = []

    def kazoo_client(hostport):
      self.opened_host_ports.append(hostport)
      return self.mock_kazoo

    self.statemanager._kazoo_client = kazoo_client

  def test_start_checks_for_connection(self):
    global did_connection_check
    did_connection_check = False

    def connection_check():
      global did_connection_check
      did_connection_check = True
      return True

    self.statemanager.is_host_port_reachable = connection_check
    self.statemanager.start()
    self.assertTrue(did_connection_check)

  def test_start_uses_host_ports(self):
    def connection_check():
      return True
    self.statemanager.is_host_port_reachable = connection_check
    self.statemanager.start()
    self.assertEqual('localhost:2181,localhost:2281',self.opened_host_ports[0])

  def test_start_opens_proxy_if_no_connection(self):
    def connection_check():
      return False

    global did_open_proxy
    did_open_proxy = False
    def open_proxy():
      global did_open_proxy
      did_open_proxy = True
      return [('proxy', 2181), ('proxy-2', 2281)]

    self.statemanager.is_host_port_reachable = connection_check
    self.statemanager.establish_ssh_tunnel = open_proxy
    self.statemanager.start()
    self.assertTrue(did_open_proxy)

  def test_proxied_start_uses_connection(self):
    def connection_check():
      return False
    def open_proxy():
      return [('smorgasboard',2200),('smorgasboard',2201)]

    self.statemanager.is_host_port_reachable = connection_check
    self.statemanager.establish_ssh_tunnel = open_proxy
    self.statemanager.start()
    self.assertEqual('smorgasboard:2200,smorgasboard:2201',self.opened_host_ports[0])
