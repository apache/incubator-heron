''' topology_helpers_unittest.py '''
# pylint: disable=missing-docstring
import unittest2 as unittest

from heron.common.src.python import constants
from heron.tools.tracker.src.python import topology_helpers
from mock_proto import MockProto

class TopologyHelpersTest(unittest.TestCase):
  def setUp(self):
    self.mock_proto = MockProto()

  def test_get_component_parallelism(self):
    topology = self.mock_proto.create_mock_medium_topology(1, 2, 3, 4)

    cmap = topology_helpers.get_component_parallelism(topology)
    self.assertEqual(1, cmap["mock_spout1"])
    self.assertEqual(2, cmap["mock_bolt1"])
    self.assertEqual(3, cmap["mock_bolt2"])
    self.assertEqual(4, cmap["mock_bolt3"])

  def test_get_disk_per_container(self):
    # We would have 9 instances
    topology = self.mock_proto.create_mock_medium_topology(1, 2, 3, 4)

    # First try with 1 container, so the disk request should be:
    # 10 * GB + Padding_Disk (12GB) = 22GB
    default_disk = topology_helpers.get_disk_per_container(topology)
    self.assertEqual(22 * constants.GB, default_disk)

    # Then try with 4 container, so the disk request should be:
    # 10/4 = 2.5 -> 3 (round to ceiling) + 12 = 15GB
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    self.assertEqual(15 * constants.GB, topology_helpers.get_disk_per_container(topology))

    # Then let's set the disk_per_container explicitly
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_CONTAINER_DISK_REQUESTED, 950109)
    # The add_topology_config will convert config into string
    self.assertEqual(str(950109), topology_helpers.get_disk_per_container(topology))

  def test_get_total_instances(self):
    topology = self.mock_proto.create_mock_medium_topology(3, 4, 5, 6)

    num_instances = topology_helpers.get_total_instances(topology)
    self.assertEqual(18, num_instances)

  def test_sane(self):
    # Make wrong topology names
    topology = self.mock_proto.create_mock_simple_topology()
    topology.name = ""
    self.assertFalse(topology_helpers.sane(topology))

    topology = self.mock_proto.create_mock_simple_topology()
    topology.name = "test.with.a.dot"
    self.assertFalse(topology_helpers.sane(topology))

    topology = self.mock_proto.create_mock_simple_topology()
    topology.name = "test/with/a/slash"
    self.assertFalse(topology_helpers.sane(topology))

    # Add another spout with the same name
    topology = self.mock_proto.create_mock_simple_topology()
    topology.spouts.extend([self.mock_proto.create_mock_spout("mock_spout", [], 1)])
    self.assertFalse(topology_helpers.sane(topology))

    # Add another bolt with the same name
    topology = self.mock_proto.create_mock_simple_topology()
    topology.bolts.extend([self.mock_proto.create_mock_bolt("mock_bolt", [], [], 1)])
    self.assertFalse(topology_helpers.sane(topology))

    # If num containers are greater than num instances
    topology = self.mock_proto.create_mock_simple_topology(1, 1)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    self.assertFalse(topology_helpers.sane(topology))

    # If rammap is partial with less componenets
    topology = self.mock_proto.create_mock_simple_topology()
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:1")
    self.assertTrue(topology_helpers.sane(topology))

    # If rammap is not well formatted
    topology = self.mock_proto.create_mock_simple_topology()
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:1:2,mock_bolt:2:3")
    self.assertFalse(topology_helpers.sane(topology))

    # If rammap has wrong component name
    topology = self.mock_proto.create_mock_simple_topology()
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "wrong_mock_spout:1,mock_bolt:2")
    self.assertFalse(topology_helpers.sane(topology))

    # If everything is right
    topology = self.mock_proto.create_mock_simple_topology()
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:1,mock_bolt:2")
    self.assertTrue(topology_helpers.sane(topology))

  def test_num_cpus_per_container(self):
    topology = self.mock_proto.create_mock_simple_topology(2, 2)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    self.assertEqual(2, topology_helpers.get_cpus_per_container(topology))

    topology = self.mock_proto.create_mock_simple_topology(2, 2)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_CONTAINER_CPU_REQUESTED, 42)
    self.assertEqual(42, topology_helpers.get_cpus_per_container(topology))

  def test_get_user_rammap(self):
    topology = self.mock_proto.create_mock_simple_topology()
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:2,mock_bolt:3")
    self.assertEqual({"mock_spout":2, "mock_bolt":3}, topology_helpers.get_user_rammap(topology))

  def test_get_component_distribution(self):
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    component_distribution = topology_helpers.get_component_distribution(topology)

    expected_component_distribution = {
        1: [
            ("mock_bolt", "1", "0"),
            ("mock_bolt", "5", "4"),
            ("mock_spout", "9", "0")
        ],
        2: [
            ("mock_bolt", "2", "1"),
            ("mock_bolt", "6", "5"),
            ("mock_spout", "10", "1")
        ],
        3: [
            ("mock_bolt", "3", "2"),
            ("mock_bolt", "7", "6"),
            ("mock_spout", "11", "2")
        ],
        4: [
            ("mock_bolt", "4", "3"),
            ("mock_bolt", "8", "7"),
            ("mock_spout", "12", "3")
        ]
    }
    self.assertEqual(expected_component_distribution, component_distribution)

  def test_get_component_rammap(self):
    # Mock a few methods
    # This is not a good way since this shows the internals
    # of the method. These methods need to be changed.
    # For example, ram_per_contaner could be taken as an argument.
    original_ram_for_stmgr = constants.RAM_FOR_STMGR
    constants.RAM_FOR_STMGR = 2

    # When rammap is specified, it should be used.
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:2,mock_bolt:3")
    self.assertEqual(
        {"mock_spout":2, "mock_bolt":3}, topology_helpers.get_component_rammap(topology))

    # When partial rammap is specified, rest of the components should get default
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:2")
    expected_component_rammap = {
        "mock_spout": 2,
        "mock_bolt": constants.DEFAULT_RAM_FOR_INSTANCE
    }
    self.assertEqual(expected_component_rammap, topology_helpers.get_component_rammap(topology))

    # When container ram is specified.
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_CONTAINER_RAM_REQUESTED, 8)

    expected_component_rammap = {
        "mock_spout": 2,
        "mock_bolt": 2
    }
    component_rammap = topology_helpers.get_component_rammap(topology)
    self.assertEqual(expected_component_rammap, component_rammap)

    # When nothing is specified.
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    component_rammap = topology_helpers.get_component_rammap(topology)
    expected_component_rammap = {
        "mock_spout": constants.DEFAULT_RAM_FOR_INSTANCE,
        "mock_bolt": constants.DEFAULT_RAM_FOR_INSTANCE
    }
    self.assertEqual(expected_component_rammap, component_rammap)

    # Unmock the things that we mocked.
    constants.RAM_FOR_STMGR = original_ram_for_stmgr

  def test_get_ram_per_container(self):
    # Mock a few things
    original_ram_for_stmgr = constants.RAM_FOR_STMGR
    constants.RAM_FOR_STMGR = 2
    original_default_ram_for_instance = constants.DEFAULT_RAM_FOR_INSTANCE
    constants.DEFAULT_RAM_FOR_INSTANCE = 1

    # When rammap is specified
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:2,mock_bolt:3")
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    self.assertEqual(10, topology_helpers.get_ram_per_container(topology))

    # When partial rammap is specified, rest of the components should get default
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_COMPONENT_RAMMAP, "mock_spout:2")
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    expected_ram_per_container = 6
    self.assertEqual(expected_ram_per_container, topology_helpers.get_ram_per_container(topology))

    # If container ram is specified
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    requested_ram = 15000
    self.mock_proto.add_topology_config(
        topology, constants.TOPOLOGY_CONTAINER_RAM_REQUESTED, str(requested_ram))
    # Difference should be less than the total instances
    self.assertLess(abs(topology_helpers.get_ram_per_container(topology) - requested_ram), 12)

    # When nothing is specified
    topology = self.mock_proto.create_mock_simple_topology(4, 8)
    self.mock_proto.add_topology_config(topology, constants.TOPOLOGY_STMGRS, 4)
    expected_ram_per_container = 5
    self.assertEqual(expected_ram_per_container, topology_helpers.get_ram_per_container(topology))

    # Unmock the things that we mocked.
    constants.RAM_FOR_STMGR = original_ram_for_stmgr
    constants.DEFAULT_RAM_FOR_INSTANCE = original_default_ram_for_instance
