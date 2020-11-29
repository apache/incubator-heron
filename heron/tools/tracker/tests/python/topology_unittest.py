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
''' topology_unittest.py '''
# pylint: disable=missing-docstring
import unittest

from heron.tools.tracker.src.python.topology import Topology
from mock_proto import MockProto

import pytest


@pytest.fixture
def topology():
  return Topology(MockProto.topology_name,
                  "test_state_manager_name")

def test_set_physical_plan(topology):
  # Set it to None
  topology.set_physical_plan(None)
  assert topology.id is None
  assert topology.physical_plan is None

  physical_plan = MockProto().create_mock_simple_physical_plan()
  topology.set_physical_plan(physical_plan)
  assert MockProto.topology_id == topology.id
  assert physical_plan == topology.physical_plan

def test_set_packing_plan(topology):
  # Set it to None
  topology.set_packing_plan(None)
  assert topology.id is None
  assert topology.packing_plan is None

  packing_plan = MockProto().create_mock_simple_packing_plan()
  topology.set_packing_plan(packing_plan)
  assert packing_plan == topology.packing_plan

  # testing with a packing plan with scheduled resources
  topology.set_packing_plan(None)
  assert topology.id is None
  assert topology.packing_plan is None

  packing_plan = MockProto().create_mock_simple_packing_plan2()
  topology.set_packing_plan(packing_plan)
  assert packing_plan == topology.packing_plan

def test_set_execution_state(topology):
  # Set it to None
  topology.set_execution_state(None)
  assert topology.execution_state is None
  assert topology.cluster is None
  assert topology.environ is None

  estate = MockProto().create_mock_execution_state()
  topology.set_execution_state(estate)
  assert estate == topology.execution_state
  assert MockProto.cluster == topology.cluster
  assert MockProto.environ == topology.environ

def test_set_tmanager(topology):
  # Set it to None
  topology.set_tmanager(None)
  assert topology.tmanager is None

  tmanager = MockProto().create_mock_tmanager()
  topology.set_tmanager(tmanager)
  assert tmanager == topology.tmanager

def test_spouts(topology):
  # When pplan is not set
  assert 0 == len(topology.spouts())

  # Set pplan now
  pplan = MockProto().create_mock_simple_physical_plan()
  topology.set_physical_plan(pplan)

  spouts = topology.spouts()
  assert 1 == len(spouts)
  assert "mock_spout" == spouts[0].comp.name
  assert ["mock_spout"] == topology.spout_names()

def test_bolts(topology):
  # When pplan is not set
  assert 0 == len(topology.bolts())

  # Set pplan
  pplan = MockProto().create_mock_medium_physical_plan()
  topology.set_physical_plan(pplan)

  bolts = topology.bolts()
  assert 3 == len(bolts)
  assert ["mock_bolt1", "mock_bolt2", "mock_bolt3"] == \
                   topology.bolt_names()

def test_num_instances(topology):
  # When pplan is not set
  assert 0 == topology.num_instances()

  pplan = MockProto().create_mock_medium_physical_plan(1, 2, 3, 4)
  topology.set_physical_plan(pplan)

  assert 10 == topology.num_instances()

def test_trigger_watches(topology):
  # Workaround
  scope = {
      "is_called": False
  }
  # pylint: disable=unused-argument, unused-variable
  def callback(something):
    scope["is_called"] = True
  uid = topology.register_watch(callback)
  assert scope["is_called"]

  scope["is_called"] = False
  assert not scope["is_called"]
  print(scope)
  topology.set_physical_plan(None)
  print(scope)
  assert scope["is_called"]
  print(scope)

  scope["is_called"] = False
  assert not scope["is_called"]
  topology.set_execution_state(None)
  assert scope["is_called"]

  scope["is_called"] = False
  assert not scope["is_called"]
  topology.set_tmanager(None)
  assert scope["is_called"]

def test_unregister_watch(topology):
  # Workaround
  scope = {
      "is_called": False
  }
  # pylint: disable=unused-argument
  def callback(something):
    scope["is_called"] = True
  uid = topology.register_watch(callback)
  scope["is_called"] = False
  assert not scope["is_called"]
  topology.set_physical_plan(None)
  assert scope["is_called"]

  topology.unregister_watch(uid)
  scope["is_called"] = False
  assert not scope["is_called"]
  topology.set_physical_plan(None)
  assert not scope["is_called"]

def test_bad_watch(topology):
  # Workaround
  scope = {
      "is_called": False
  }
  # pylint: disable=unused-argument, unused-variable
  def callback(something):
    scope["is_called"] = True
    raise Exception("Test Bad Trigger Exception")

  uid = topology.register_watch(callback)
  # is called the first time because of registeration
  assert scope["is_called"]

  # But should no longer be called
  scope["is_called"] = False
  assert not scope["is_called"]
  topology.set_physical_plan(None)
  assert not scope["is_called"]
