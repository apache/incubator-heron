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

'''integration test topology builder'''
import copy
import heronpy.api.api_constants as api_constants
from heronpy.api.stream import Stream, Grouping
from heronpy.api.topology import TopologyBuilder, Topology, TopologyType
from ..core import constants as integ_const
from .aggregator_bolt import AggregatorBolt
from .integration_test_spout import IntegrationTestSpout
from .integration_test_bolt import IntegrationTestBolt

class TestTopologyBuilder(TopologyBuilder):
  """Topology Builder for integration tests

  Given spouts and bolts will be delegated by IntegrationTestSpout and IntegrationTestBolt
  classes respectively.
  """
  TERMINAL_BOLT_NAME = '__integration_test_aggregator_bolt'
  TERMINAL_BOLT_CLASS = AggregatorBolt
  DEFAULT_CONFIG = {api_constants.TOPOLOGY_DEBUG: True,
                    api_constants.TOPOLOGY_RELIABILITY_MODE:
                    api_constants.TopologyReliabilityMode.ATLEAST_ONCE,
                    api_constants.TOPOLOGY_PROJECT_NAME: "heron-integration-test"}
  def __init__(self, name, http_server_url):
    super(TestTopologyBuilder, self).__init__(name)
    self.output_location = "%s/%s" % (http_server_url, self.topology_name)
    self.set_config(self.DEFAULT_CONFIG)

    # map <name -> spout's component spec>
    self.spouts = {}
    # map <name -> bolt's component spec>
    self.bolts = {}
    # map <name -> set of parents>
    self.prev = {}

  def add_spout(self, name, spout_cls, par, config=None,
                optional_outputs=None, max_executions=None):
    """Add an integration_test spout"""
    user_spec = spout_cls.spec(name)
    spout_classpath = user_spec.python_class_path

    if hasattr(spout_cls, 'outputs'):
      user_outputs = spout_cls.outputs
    else:
      user_outputs = []

    if optional_outputs is not None:
      user_outputs.extend(optional_outputs)

    if config is None:
      _config = {}
    else:
      _config = copy.copy(config)

    if max_executions is not None:
      _config[integ_const.USER_MAX_EXECUTIONS] = max_executions

    test_spec = IntegrationTestSpout.spec(name, par, _config,
                                          user_spout_classpath=spout_classpath,
                                          user_output_fields=user_outputs)
    self.add_spec(test_spec)
    self.spouts[name] = test_spec
    return test_spec

  def add_bolt(self, name, bolt_cls, par, inputs, config=None, optional_outputs=None):
    """Add an integration_test bolt

    Only dict based inputs is supported
    """
    assert isinstance(inputs, dict)
    user_spec = bolt_cls.spec(name)
    bolt_classpath = user_spec.python_class_path

    if hasattr(bolt_cls, 'outputs'):
      user_outputs = bolt_cls.outputs
    else:
      user_outputs = []

    if optional_outputs is not None:
      user_outputs.extend(optional_outputs)

    if config is None:
      _config = {}
    else:
      _config = config

    test_spec = IntegrationTestBolt.spec(name, par, inputs, _config,
                                         user_bolt_classpath=bolt_classpath,
                                         user_output_fields=user_outputs)
    self.add_spec(test_spec)
    self.bolts[name] = test_spec
    return test_spec

  # pylint: disable=too-many-branches
  def create_topology(self):
    """Creates an integration_test topology class"""

    # first add the aggregation_bolt
    # inputs will be updated later
    aggregator_config = {integ_const.HTTP_POST_URL_KEY: self.output_location}
    self.add_bolt(self.TERMINAL_BOLT_NAME, self.TERMINAL_BOLT_CLASS, 1,
                  inputs={}, config=aggregator_config)

    # building a graph directed from children to parents, by looking only on bolts
    # since spouts don't have parents
    for name, bolt_spec in self.bolts.items():
      if name == self.TERMINAL_BOLT_NAME:
        continue

      bolt_protobuf = bolt_spec.get_protobuf()
      for istream in bolt_protobuf.inputs:
        parent = istream.stream.component_name
        if name in self.prev:
          self.prev[name].add(parent)
        else:
          parents = set()
          parents.add(parent)
          self.prev[name] = parents

    # Find the terminal bolts defined by users and link them with "AggregatorBolt".

    # set of terminal component names
    terminals = set()
    # set of non-terminal component names
    non_terminals = set()
    # 1. terminal bolts need upstream components, because we don't want isolated bolts
    # 2. terminal bolts should not exist in the prev.values(), meaning that no downstream
    for parent_set in list(self.prev.values()):
      non_terminals.update(parent_set)

    for bolt_name in list(self.prev.keys()):
      if bolt_name not in non_terminals:
        terminals.add(bolt_name)

    # will also consider the cases with spouts without children
    for spout_name in list(self.spouts.keys()):
      if spout_name not in non_terminals:
        terminals.add(spout_name)

    # add all grouping to components
    for child in list(self.prev.keys()):
      for parent in self.prev[child]:
        self._add_all_grouping(child, parent, integ_const.INTEGRATION_TEST_CONTROL_STREAM_ID)

    # then connect aggregator bolt with user's terminal components
    # terminal_outputs are output fields for terminals, list of either str or Stream
    for terminal in terminals:
      if terminal in self.bolts:
        terminal_outputs = self.bolts[terminal].outputs
      else:
        terminal_outputs = self.spouts[terminal].outputs

      # now get a set of stream ids
      stream_ids = ["default" if isinstance(out, str) else out.stream_id
                    for out in terminal_outputs]
      for stream_id in set(stream_ids):
        self._add_all_grouping(self.TERMINAL_BOLT_NAME, terminal, stream_id)

    # create topology class
    class_dict = self._construct_topo_class_dict()
    return TopologyType(self.topology_name, (Topology,), class_dict)

  def _add_all_grouping(self, child, parent, stream_id):
    """Adds all grouping between child component and parent component with a given stream id

    :type child: str
    :param child: child's component name
    :type parent: str
    :param parent: parent's component name
    :type stream_id: str
    :param stream_id: stream id
    """
    # child has to be a bolt
    child_component_spec = self.bolts[child]
    # child_inputs is dict mapping from <HeronComponentSpec|GlobalStreamId -> grouping>
    child_inputs = child_component_spec.inputs

    if parent in self.bolts:
      parent_component_spec = self.bolts[parent]
    else:
      parent_component_spec = self.spouts[parent]

    if stream_id == Stream.DEFAULT_STREAM_ID:
      child_inputs[parent_component_spec] = Grouping.ALL
    else:
      child_inputs[parent_component_spec[stream_id]] = Grouping.ALL
