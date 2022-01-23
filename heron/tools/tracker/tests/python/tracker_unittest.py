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
from functools import partial
from unittest.mock import call, patch, Mock

import heron.proto.execution_state_pb2 as protoEState

from heron.statemgrs.src.python import statemanagerfactory
from heron.tools.tracker.src.python.topology import Topology
from heron.tools.tracker.src.python.tracker import Tracker

import pytest

# just for convenience in testing
Topology = partial(Topology, tracker_config={})

@pytest.fixture
def tracker():
  mock_config = Mock()
  mock_config.validate.return_value = True
  return Tracker(mock_config)

@pytest.fixture
def mock_tracker(tracker):
  # this wouldn't be so ugly with Python3.9+
  with patch.object(Tracker, 'get_stmgr_topologies'), patch.object(Tracker, 'remove_topology'), patch.object(Tracker, 'add_new_topology'), patch.object(statemanagerfactory, 'get_all_state_managers'):
    yield tracker

# pylint: disable=unused-argument
def test_first_sync_topologies(mock_tracker):
  mock_state_manager_1 = Mock()
  mock_state_manager_1.name = 'mock_name1'

  mock_state_manager_2 = Mock()
  mock_state_manager_2.name = 'mock_name2'

  watches = {}
  statemanagerfactory.get_all_state_managers.return_value = [mock_state_manager_1, mock_state_manager_2]

  mock_tracker.get_stmgr_topologies.return_value = []
  def side_effect1(on_topologies_watch):
    watches["1"] = on_topologies_watch
    on_topologies_watch(['top_name1', 'top_name2'])
  mock_state_manager_1.get_topologies = side_effect1

  def side_effect2(on_topologies_watch):
    watches["2"] = on_topologies_watch
    on_topologies_watch(['top_name3', 'top_name4'])
  mock_state_manager_2.get_topologies = side_effect2

  mock_tracker.sync_topologies()
  mock_tracker.get_stmgr_topologies.assert_has_calls(
      [call("mock_name2"),
       call("mock_name1")],
      any_order=True)
  mock_tracker.add_new_topology.assert_has_calls([call(mock_state_manager_1, 'top_name1'),
                                          call(mock_state_manager_1, 'top_name2'),
                                          call(mock_state_manager_2, 'top_name3'),
                                          call(mock_state_manager_2, 'top_name4')],
                                         any_order=True)

def test_sync_topologies_leading_with_add_and_remove_topologies(mock_tracker):
  mock_state_manager_1 = Mock()
  mock_state_manager_1.name = 'mock_name1'

  mock_state_manager_2 = Mock()
  mock_state_manager_2.name = 'mock_name2'

  watches = {}
  statemanagerfactory.get_all_state_managers.return_value = [mock_state_manager_1, mock_state_manager_2]
  mock_tracker.get_stmgr_topologies.return_value = []

  def side_effect1(on_topologies_watch):
    watches["1"] = on_topologies_watch
    on_topologies_watch(['top_name1', 'top_name2'])
  mock_state_manager_1.get_topologies = side_effect1

  def side_effect2(on_topologies_watch):
    watches["2"] = on_topologies_watch
    on_topologies_watch(['top_name3', 'top_name4'])
  mock_state_manager_2.get_topologies = side_effect2

  mock_tracker.sync_topologies()
  mock_tracker.get_stmgr_topologies.assert_has_calls(
      [call("mock_name2"),
       call("mock_name1")],
      any_order=True)
  mock_tracker.add_new_topology.assert_has_calls([call(mock_state_manager_1, 'top_name1'),
                                          call(mock_state_manager_1, 'top_name2'),
                                          call(mock_state_manager_2, 'top_name3'),
                                          call(mock_state_manager_2, 'top_name4')],
                                         any_order=True)
  assert 4 == mock_tracker.add_new_topology.call_count
  assert 0 == mock_tracker.remove_topology.call_count
  mock_tracker.get_stmgr_topologies.reset_mock()
  mock_tracker.add_new_topology.reset_mock()
  mock_tracker.remove_topology.reset_mock()

  def get_topologies_for_state_location_side_effect(name):
    if name == 'mock_name1':
      return [Topology('top_name1', 'mock_name1'),
              Topology('top_name2', 'mock_name1')]
    if name == 'mock_name2':
      return [Topology('top_name3', 'mock_name2'),
              Topology('top_name4', 'mock_name2')]
    return []

  # pylint: disable=line-too-long
  mock_tracker.get_stmgr_topologies.side_effect = get_topologies_for_state_location_side_effect

  watches["1"](['top_name1', 'top_name3'])
  watches["2"](['top_name5', 'top_name6'])
  mock_tracker.add_new_topology.assert_has_calls([call(mock_state_manager_1, 'top_name3'),
                                          call(mock_state_manager_2, 'top_name5'),
                                          call(mock_state_manager_2, 'top_name6')],
                                         any_order=True)
  mock_tracker.remove_topology.assert_has_calls([call('top_name2', 'mock_name1'),
                                         call('top_name3', 'mock_name2'),
                                         call('top_name4', 'mock_name2')],
                                        any_order=True)
  assert 3 == mock_tracker.add_new_topology.call_count
  assert 3 == mock_tracker.remove_topology.call_count

@pytest.fixture
def topologies(tracker):

  def create_mock_execution_state(cluster, role, environ):
    estate = protoEState.ExecutionState()
    estate.cluster = cluster
    estate.role = role
    estate.environ = environ
    return estate

  topology1 = Topology('top_name1', 'mock_name1')
  topology1.execution_state = create_mock_execution_state('cluster1', 'mark', 'env1')

  topology2 = Topology('top_name2', 'mock_name1')
  topology2.execution_state = create_mock_execution_state('cluster1', 'bob', 'env1')

  topology3 = Topology('top_name3', 'mock_name1')
  topology3.execution_state = create_mock_execution_state('cluster1', 'tom', 'env2')

  topology4 = Topology('top_name4', 'mock_name2')
  topology4.execution_state = create_mock_execution_state('cluster2', 'x', 'env1')

  topology5 = Topology('top_name5', 'mock_name2')
  topology5.execution_state = create_mock_execution_state('cluster2', 'x', 'env2')
  tracker.topologies = [
      topology1,
      topology2,
      topology3,
      topology4,
      topology5]
  return tracker.topologies[:]

# pylint: disable=line-too-long
def test_get_topology_by_cluster_environ_and_name(tracker, topologies):
  assert topologies[0] == tracker.get_topology('cluster1', 'mark', 'env1', 'top_name1')
  assert topologies[0] == tracker.get_topology('cluster1', None, 'env1', 'top_name1')
  assert topologies[1] == tracker.get_topology('cluster1', 'bob', 'env1', 'top_name2')
  assert topologies[1] == tracker.get_topology('cluster1', None, 'env1', 'top_name2')
  assert topologies[2] == tracker.get_topology('cluster1', 'tom', 'env2', 'top_name3')
  assert topologies[2] == tracker.get_topology('cluster1', None, 'env2', 'top_name3')
  assert topologies[3] == tracker.get_topology('cluster2', None, 'env1', 'top_name4')
  assert topologies[4] == tracker.get_topology('cluster2', None, 'env2', 'top_name5')

def test_get_topolies_for_state_location(tracker, topologies):
  assert [topologies[0], topologies[1], topologies[2]] == tracker.get_stmgr_topologies('mock_name1')
  assert [topologies[3], topologies[4]] == tracker.get_stmgr_topologies('mock_name2')

def test_add_new_topology(tracker):
  assert [] == tracker.topologies
  mock_state_manager_1 = Mock()
  mock_state_manager_1.name = 'mock_name1'

  tracker.add_new_topology(mock_state_manager_1, 'top_name1')
  assert ['top_name1'] == [t.name for t in tracker.topologies]

  tracker.add_new_topology(mock_state_manager_1, 'top_name2')
  assert ['top_name1', 'top_name2'] == [t.name for t in tracker.topologies]

  assert 2 == mock_state_manager_1.get_pplan.call_count
  assert 2 == mock_state_manager_1.get_execution_state.call_count
  assert 2 == mock_state_manager_1.get_tmanager.call_count

def test_remove_topology(tracker, topologies):
  tracker.remove_topology('top_name1', 'mock_name1')
  assert [topologies[1], topologies[2], topologies[3], topologies[4]] == tracker.topologies
  tracker.remove_topology('top_name2', 'mock_name1')
  assert [topologies[2], topologies[3], topologies[4]] == tracker.topologies
  # Removing one that is not there should not have any affect
  tracker.remove_topology('top_name8', 'mock_name1')
  assert [topologies[2], topologies[3], topologies[4]] == tracker.topologies
  tracker.remove_topology('top_name4', 'mock_name2')
  assert [topologies[2], topologies[4]] == tracker.topologies
