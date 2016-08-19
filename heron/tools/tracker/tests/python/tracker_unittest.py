''' tracker_unittest.py '''

# pylint: disable=missing-docstring, attribute-defined-outside-init
import unittest2 as unittest
from mock import call, patch, Mock

import heron.proto.execution_state_pb2 as protoEState
from heron.statemgrs.src.python import statemanagerfactory
from heron.tools.tracker.src.python.topology import Topology
from heron.tools.tracker.src.python.tracker import Tracker

class TrackerTest(unittest.TestCase):
  def setUp(self):
    mock_config = Mock()
    mock_config.validate.return_value = True
    self.tracker = Tracker(mock_config)

  # pylint: disable=unused-argument
  @patch.object(Tracker, 'getTopologiesForStateLocation')
  @patch.object(Tracker, 'removeTopology')
  @patch.object(Tracker, 'addNewTopology')
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

  @patch.object(Tracker, 'getTopologiesForStateLocation')
  @patch.object(Tracker, 'removeTopology')
  @patch.object(Tracker, 'addNewTopology')
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
    self.assertEqual(4, mock_add_new_topology.call_count)
    self.assertEqual(0, mock_remove_topology.call_count)
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
    self.assertEqual(3, mock_add_new_topology.call_count)
    self.assertEqual(3, mock_remove_topology.call_count)

  def fill_tracker_topologies(self):

    def create_mock_execution_state(role):
      estate = protoEState.ExecutionState()
      estate.role = role
      return estate

    self.topology1 = Topology('top_name1', 'mock_name1')
    self.topology1.cluster = 'cluster1'
    self.topology1.environ = 'env1'
    self.topology1.execution_state = create_mock_execution_state('mark')

    self.topology2 = Topology('top_name2', 'mock_name1')
    self.topology2.cluster = 'cluster1'
    self.topology2.environ = 'env1'
    self.topology2.execution_state = create_mock_execution_state('bob')

    self.topology3 = Topology('top_name3', 'mock_name1')
    self.topology3.cluster = 'cluster1'
    self.topology3.environ = 'env2'
    self.topology3.execution_state = create_mock_execution_state('tom')

    self.topology4 = Topology('top_name4', 'mock_name2')
    self.topology4.cluster = 'cluster2'
    self.topology4.environ = 'env1'

    self.topology5 = Topology('top_name5', 'mock_name2')
    self.topology5.cluster = 'cluster2'
    self.topology5.environ = 'env2'
    self.tracker.topologies = [
        self.topology1,
        self.topology2,
        self.topology3,
        self.topology4,
        self.topology5]

  # pylint: disable=line-too-long
  def test_get_topology_by_cluster_environ_and_name(self):
    self.fill_tracker_topologies()
    self.assertEqual(self.topology1, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster1', 'mark', 'env1', 'top_name1'))
    self.assertEqual(self.topology1, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster1', None, 'env1', 'top_name1'))
    self.assertEqual(self.topology2, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster1', 'bob', 'env1', 'top_name2'))
    self.assertEqual(self.topology2, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster1', None, 'env1', 'top_name2'))
    self.assertEqual(self.topology3, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster1', 'tom', 'env2', 'top_name3'))
    self.assertEqual(self.topology3, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster1', None, 'env2', 'top_name3'))
    self.assertEqual(self.topology4, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster2', None, 'env1', 'top_name4'))
    self.assertEqual(self.topology5, self.tracker.getTopologyByClusterRoleEnvironAndName('cluster2', None, 'env2', 'top_name5'))

  def test_get_topolies_for_state_location(self):
    self.fill_tracker_topologies()
    self.assertItemsEqual(
        [self.topology1, self.topology2, self.topology3],
        self.tracker.getTopologiesForStateLocation('mock_name1'))
    self.assertItemsEqual(
        [self.topology4, self.topology5],
        self.tracker.getTopologiesForStateLocation('mock_name2'))

  def test_add_new_topology(self):
    self.assertItemsEqual([], self.tracker.topologies)
    mock_state_manager_1 = Mock()
    mock_state_manager_1.name = 'mock_name1'

    self.tracker.addNewTopology(mock_state_manager_1, 'top_name1')
    self.assertItemsEqual(
        ['top_name1'],
        map(lambda t: t.name, self.tracker.topologies))

    self.tracker.addNewTopology(mock_state_manager_1, 'top_name2')
    self.assertItemsEqual(
        ['top_name1', 'top_name2'],
        map(lambda t: t.name, self.tracker.topologies))

    self.assertEqual(2, mock_state_manager_1.get_pplan.call_count)
    self.assertEqual(2, mock_state_manager_1.get_execution_state.call_count)
    self.assertEqual(2, mock_state_manager_1.get_tmaster.call_count)

  def test_remove_topology(self):
    self.fill_tracker_topologies()
    self.tracker.removeTopology('top_name1', 'mock_name1')
    self.assertItemsEqual([self.topology2, self.topology3, self.topology4, self.topology5],
                          self.tracker.topologies)
    self.tracker.removeTopology('top_name2', 'mock_name1')
    self.assertItemsEqual([self.topology3, self.topology4, self.topology5],
                          self.tracker.topologies)
    # Removing one that is not there should not have any affect
    self.tracker.removeTopology('top_name8', 'mock_name1')
    self.assertItemsEqual([self.topology3, self.topology4, self.topology5],
                          self.tracker.topologies)
    self.tracker.removeTopology('top_name4', 'mock_name2')
    self.assertItemsEqual([self.topology3, self.topology5],
                          self.tracker.topologies)
