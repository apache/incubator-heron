# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
''' tracker_unittest.py '''

# pylint: disable=missing-docstring, attribute-defined-outside-init
import unittest

from functools import partial
from unittest.mock import call, patch, Mock

import heron.proto.execution_state_pb2 as protoEState
from heron.statemgrs.src.python import statemanagerfactory
from heron.tools.tracker.src.python.topology import Topology
from heron.tools.tracker.src.python.tracker import Tracker

Topology = partial(Topology, tracker_config={})

class TrackerTest(unittest.TestCase):
  def setUp(self):
    mock_config = Mock()
    mock_config.validate.return_value = True
    self.tracker = Tracker(mock_config)

  # pylint: disable=unused-argument
  @patch.object(Tracker, 'get_stmgr_topologies')
  @patch.object(Tracker, 'remove_topology')
  @patch.object(Tracker, 'add_new_topology')
  @patch.object(statemanagerfactory, 'get_all_state_managers')
  def test_first_synch_topologies(
      self, mock_get_all_state_managers,
      mock_add_new_topology, mock_remove_topology,
      mock_get_topologies_for_state_location):
    mock_state_manager_1 = Mock()
    mock_state_manager_1.name = 'mock_name1'

    mock_state_manager_2 = Mock()
    mock_state_manager_2.name = 'mock_name2'

    watches = {}
    mock_get_all_state_managers.return_value = [mock_state_manager_1, mock_state_manager_2]

    mock_get_topologies_for_state_location.return_value = []
    def side_effect1(on_topologies_watch):
      watches["1"] = on_topologies_watch
      on_topologies_watch(['top_name1', 'top_name2'])
    mock_state_manager_1.get_topologies = side_effect1

    def side_effect2(on_topologies_watch):
      watches["2"] = on_topologies_watch
      on_topologies_watch(['top_name3', 'top_name4'])
    mock_state_manager_2.get_topologies = side_effect2

    self.tracker.synch_topologies()
    mock_get_topologies_for_state_location.assert_has_calls(
        [call("mock_name2"),
         call("mock_name1")],
        any_order=True)
    mock_add_new_topology.assert_has_calls([call(mock_state_manager_1, 'top_name1'),
                                            call(mock_state_manager_1, 'top_name2'),
                                            call(mock_state_manager_2, 'top_name3'),
                                            call(mock_state_manager_2, 'top_name4')],
                                           any_order=True)

  @patch.object(Tracker, 'get_stmgr_topologies')
  @patch.object(Tracker, 'remove_topology')
  @patch.object(Tracker, 'add_new_topology')
  @patch.object(statemanagerfactory, 'get_all_state_managers')
  def test_synch_topologies_leading_with_add_and_remove_topologies(
      self, mock_get_all_state_managers,
      mock_add_new_topology, mock_remove_topology,
      mock_get_topologies_for_state_location):
    mock_state_manager_1 = Mock()
    mock_state_manager_1.name = 'mock_name1'

    mock_state_manager_2 = Mock()
    mock_state_manager_2.name = 'mock_name2'

    watches = {}
    mock_get_all_state_managers.return_value = [mock_state_manager_1, mock_state_manager_2]
    mock_get_topologies_for_state_location.return_value = []

    def side_effect1(on_topologies_watch):
      watches["1"] = on_topologies_watch
      on_topologies_watch(['top_name1', 'top_name2'])
    mock_state_manager_1.get_topologies = side_effect1

    def side_effect2(on_topologies_watch):
      watches["2"] = on_topologies_watch
      on_topologies_watch(['top_name3', 'top_name4'])
    mock_state_manager_2.get_topologies = side_effect2

    self.tracker.synch_topologies()
    mock_get_topologies_for_state_location.assert_has_calls(
        [call("mock_name2"),
         call("mock_name1")],
        any_order=True)
    mock_add_new_topology.assert_has_calls([call(mock_state_manager_1, 'top_name1'),
                                            call(mock_state_manager_1, 'top_name2'),
                                            call(mock_state_manager_2, 'top_name3'),
                                            call(mock_state_manager_2, 'top_name4')],
                                           any_order=True)
    assert 4 == mock_add_new_topology.call_count
    assert 0 == mock_remove_topology.call_count
    mock_get_topologies_for_state_location.reset_mock()
    mock_add_new_topology.reset_mock()
    mock_remove_topology.reset_mock()

    def get_topologies_for_state_location_side_effect(name):
      if name == 'mock_name1':
        return [Topology('top_name1', 'mock_name1'),
                Topology('top_name2', 'mock_name1')]
      if name == 'mock_name2':
        return [Topology('top_name3', 'mock_name2'),
                Topology('top_name4', 'mock_name2')]
      return []

    # pylint: disable=line-too-long
    mock_get_topologies_for_state_location.side_effect = get_topologies_for_state_location_side_effect

    watches["1"](['top_name1', 'top_name3'])
    watches["2"](['top_name5', 'top_name6'])
    mock_add_new_topology.assert_has_calls([call(mock_state_manager_1, 'top_name3'),
                                            call(mock_state_manager_2, 'top_name5'),
                                            call(mock_state_manager_2, 'top_name6')],
                                           any_order=True)
    mock_remove_topology.assert_has_calls([call('top_name2', 'mock_name1'),
                                           call('top_name3', 'mock_name2'),
                                           call('top_name4', 'mock_name2')],
                                          any_order=False)
    assert 3 == mock_add_new_topology.call_count
    assert 3 == mock_remove_topology.call_count

  def fill_tracker_topologies(self):

    def create_mock_execution_state(cluster, role, environ):
      estate = protoEState.ExecutionState()
      estate.cluster = cluster
      estate.role = role
      estate.environ = environ
      return estate

    self.topology1 = Topology('top_name1', 'mock_name1')
    self.topology1.execution_state = create_mock_execution_state('cluster1', 'mark', 'env1')

    self.topology2 = Topology('top_name2', 'mock_name1')
    self.topology2.execution_state = create_mock_execution_state('cluster1', 'bob', 'env1')

    self.topology3 = Topology('top_name3', 'mock_name1')
    self.topology3.execution_state = create_mock_execution_state('cluster1', 'tom', 'env2')

    self.topology4 = Topology('top_name4', 'mock_name2')
    self.topology4.execution_state = create_mock_execution_state('cluster2', 'x', 'env1')

    self.topology5 = Topology('top_name5', 'mock_name2')
    self.topology5.execution_state = create_mock_execution_state('cluster2', 'x', 'env2')
    self.tracker.topologies = [
        self.topology1,
        self.topology2,
        self.topology3,
        self.topology4,
        self.topology5]

  # pylint: disable=line-too-long
  def test_get_topology_by_cluster_environ_and_name(self):
    self.fill_tracker_topologies()
    assert self.topology1 == self.tracker.get_topology('cluster1', 'mark', 'env1', 'top_name1')
    assert self.topology1 == self.tracker.get_topology('cluster1', None, 'env1', 'top_name1')
    assert self.topology2 == self.tracker.get_topology('cluster1', 'bob', 'env1', 'top_name2')
    assert self.topology2 == self.tracker.get_topology('cluster1', None, 'env1', 'top_name2')
    assert self.topology3 == self.tracker.get_topology('cluster1', 'tom', 'env2', 'top_name3')
    assert self.topology3 == self.tracker.get_topology('cluster1', None, 'env2', 'top_name3')
    assert self.topology4 == self.tracker.get_topology('cluster2', None, 'env1', 'top_name4')
    assert self.topology5 == self.tracker.get_topology('cluster2', None, 'env2', 'top_name5')

  def test_get_topolies_for_state_location(self):
    self.fill_tracker_topologies()
    self.assertCountEqual(
        [self.topology1, self.topology2, self.topology3],
        self.tracker.get_stmgr_topologies('mock_name1'))
    self.assertCountEqual(
        [self.topology4, self.topology5],
        self.tracker.get_stmgr_topologies('mock_name2'))

  def test_add_new_topology(self):
    self.assertCountEqual([], self.tracker.topologies)
    mock_state_manager_1 = Mock()
    mock_state_manager_1.name = 'mock_name1'

    self.tracker.add_new_topology(mock_state_manager_1, 'top_name1')
    self.assertCountEqual(
        ['top_name1'],
        [t.name for t in self.tracker.topologies])

    self.tracker.add_new_topology(mock_state_manager_1, 'top_name2')
    self.assertCountEqual(
        ['top_name1', 'top_name2'],
        [t.name for t in self.tracker.topologies])

    assert 2 == mock_state_manager_1.get_pplan.call_count
    assert 2 == mock_state_manager_1.get_execution_state.call_count
    assert 2 == mock_state_manager_1.get_tmanager.call_count

  def test_remove_topology(self):
    self.fill_tracker_topologies()
    self.tracker.remove_topology('top_name1', 'mock_name1')
    self.assertCountEqual([self.topology2, self.topology3, self.topology4, self.topology5],
                          self.tracker.topologies)
    self.tracker.remove_topology('top_name2', 'mock_name1')
    self.assertCountEqual([self.topology3, self.topology4, self.topology5],
                          self.tracker.topologies)
    # Removing one that is not there should not have any affect
    self.tracker.remove_topology('top_name8', 'mock_name1')
    self.assertCountEqual([self.topology3, self.topology4, self.topology5],
                          self.tracker.topologies)
    self.tracker.remove_topology('top_name4', 'mock_name2')
    self.assertCountEqual([self.topology3, self.topology5],
                          self.tracker.topologies)
