''' topology_unittest.py '''
# pylint: disable=missing-docstring
import unittest2 as unittest

from heron.tools.tracker.src.python.topology import Topology
from mock_proto import MockProto

class TopologyTest(unittest.TestCase):
  def setUp(self):
    self.state_manager_name = "test_state_manager_name"
    self.topology = Topology(MockProto.topology_name,
                             self.state_manager_name)

  def test_set_physical_plan(self):
    # Set it to None
    self.topology.set_physical_plan(None)
    self.assertIsNone(self.topology.id)
    self.assertIsNone(self.topology.physical_plan)

    physical_plan = MockProto().create_mock_simple_physical_plan()
    self.topology.set_physical_plan(physical_plan)
    self.assertEqual(MockProto.topology_id, self.topology.id)
    self.assertEqual(physical_plan, self.topology.physical_plan)

  def test_set_execution_state(self):
    # Set it to None
    self.topology.set_execution_state(None)
    self.assertIsNone(self.topology.execution_state)
    self.assertIsNone(self.topology.cluster)
    self.assertIsNone(self.topology.environ)

    estate = MockProto().create_mock_execution_state()
    self.topology.set_execution_state(estate)
    self.assertEqual(estate, self.topology.execution_state)
    self.assertEqual(MockProto.cluster, self.topology.cluster)
    self.assertEqual(MockProto.environ, self.topology.environ)

  def test_set_tmaster(self):
    # Set it to None
    self.topology.set_tmaster(None)
    self.assertIsNone(self.topology.tmaster)

    tmaster = MockProto().create_mock_tmaster()
    self.topology.set_tmaster(tmaster)
    self.assertEqual(tmaster, self.topology.tmaster)

  def test_spouts(self):
    # When pplan is not set
    self.assertEqual(0, len(self.topology.spouts()))

    # Set pplan now
    pplan = MockProto().create_mock_simple_physical_plan()
    self.topology.set_physical_plan(pplan)

    spouts = self.topology.spouts()
    self.assertEqual(1, len(spouts))
    self.assertEqual("mock_spout", spouts[0].comp.name)
    self.assertEqual(["mock_spout"], self.topology.spout_names())

  def test_bolts(self):
    # When pplan is not set
    self.assertEqual(0, len(self.topology.bolts()))

    # Set pplan
    pplan = MockProto().create_mock_medium_physical_plan()
    self.topology.set_physical_plan(pplan)

    bolts = self.topology.bolts()
    self.assertEqual(3, len(bolts))
    self.assertEqual(["mock_bolt1", "mock_bolt2", "mock_bolt3"],
                     self.topology.bolt_names())

  def test_num_instances(self):
    # When pplan is not set
    self.assertEqual(0, self.topology.num_instances())

    pplan = MockProto().create_mock_medium_physical_plan(1, 2, 3, 4)
    self.topology.set_physical_plan(pplan)

    self.assertEqual(10, self.topology.num_instances())

  def test_trigger_watches(self):
    # Workaround
    scope = {
        "is_called": False
    }
    # pylint: disable=unused-argument, unused-variable
    def callback(something):
      scope["is_called"] = True
    uid = self.topology.register_watch(callback)
    self.assertTrue(scope["is_called"])

    scope["is_called"] = False
    self.assertFalse(scope["is_called"])
    print scope
    self.topology.set_physical_plan(None)
    print scope
    self.assertTrue(scope["is_called"])
    print scope

    scope["is_called"] = False
    self.assertFalse(scope["is_called"])
    self.topology.set_execution_state(None)
    self.assertTrue(scope["is_called"])

    scope["is_called"] = False
    self.assertFalse(scope["is_called"])
    self.topology.set_tmaster(None)
    self.assertTrue(scope["is_called"])

  def test_unregister_watch(self):
    # Workaround
    scope = {
        "is_called": False
    }
    # pylint: disable=unused-argument
    def callback(something):
      scope["is_called"] = True
    uid = self.topology.register_watch(callback)
    scope["is_called"] = False
    self.assertFalse(scope["is_called"])
    self.topology.set_physical_plan(None)
    self.assertTrue(scope["is_called"])

    self.topology.unregister_watch(uid)
    scope["is_called"] = False
    self.assertFalse(scope["is_called"])
    self.topology.set_physical_plan(None)
    self.assertFalse(scope["is_called"])

  def test_bad_watch(self):
    # Workaround
    scope = {
        "is_called": False
    }
    # pylint: disable=unused-argument, unused-variable
    def callback(something):
      scope["is_called"] = True
      raise Exception("Test Bad Trigger Exception")

    uid = self.topology.register_watch(callback)
    # is called the first time because of registeration
    self.assertTrue(scope["is_called"])

    # But should no longer be called
    scope["is_called"] = False
    self.assertFalse(scope["is_called"])
    self.topology.set_physical_plan(None)
    self.assertFalse(scope["is_called"])
