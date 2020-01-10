#!/usr/bin/env python3
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

''' explorer_unittest.py '''
import json
import os
import unittest2 as unittest
from mock import Mock

import heron.tools.common.src.python.access.tracker_access as tracker_access
import heron.tools.explorer.src.python.topologies as topologies
import heron.tools.explorer.src.python.main as main
import heron.tools.explorer.src.python.args as args


# pylint: disable=missing-docstring, no-self-use
class ExplorerTest(unittest.TestCase):
  ''' unit tests '''
  def setUp(self):
    base_path = os.path.dirname(os.path.realpath(__file__))
    info_path = os.path.join(base_path, 'info.json')
    lp_path = os.path.join(base_path, 'logicalplan.json')
    topo_path = os.path.join(base_path, 'topologies.json')
    metrics_path = os.path.join(base_path, 'metrics.json')
    with open(info_path) as f:
      tracker_access.get_topology_info = Mock(return_value=json.load(f))
    with open(lp_path) as f:
      tracker_access.get_logical_plan = Mock(return_value=json.load(f))
    with open(topo_path) as f:
      j = json.load(f)
      tracker_access.get_cluster_topologies = Mock(return_value=j)
      tracker_access.get_cluster_role_topologies = Mock(return_value=j)
      tracker_access.get_cluster_role_env_topologies = Mock(return_value=j)
    with open(metrics_path) as f:
      tracker_access.get_topology_metrics = Mock(return_value=json.load(f))
    clusters = ['nyc', 'london', 'tokyo']
    tracker_access.get_clusters = Mock(return_value=clusters)

  def sample_topo_result(self):
    info = []
    info.append(['a1', 'a2', 'a3'])
    info.append(['a1', 'a3', 'a5'])
    info.append(['a1', 'a3', 'a6'])
    info.append(['a2', 'a3', 'a6'])
    info.append(['b1', 'b2', 'b3'])
    info.append(['c1', 'c2', 'c3'])
    d = {}
    for row in info:
      tmp = d
      if row[0] not in tmp:
        tmp[row[0]] = {}
      tmp = tmp[row[0]]
      if row[1] not in tmp:
        tmp[row[1]] = []
      tmp[row[1]].append(row[2])
    return d, info

  def acc_with_optional_args(self, cl, acc):
    clv = cl + ['--verbose']
    clt = cl + ['--tracker-url="http://a.com"']
    cltv = clv + ['--tracker-url="http://a.com"']
    for cl in clv, clt, cltv:
      acc.append(args.insert_bool_values(cl))

  def sample_topo_cls(self):
    clt1 = ['topologies', 'local']
    clt2 = ['topologies', 'local/foo']
    clt3 = ['topologies', 'local/foo/bar']
    all_cl = []
    for cl in clt1, clt2, clt3:
      self.acc_with_optional_args(cl, all_cl)
    return all_cl

  def sample_lp_cls(self):
    clco = ['components', 'local/rli/default', 'ExclamationTopology']
    clsp = ['components', 'local/rli/default', 'ExclamationTopology', '--spout']
    clbo = ['components', 'local/rli/default', 'ExclamationTopology', '--bolt']
    good_cls = []
    for cl in clsp, clbo, clco:
      self.acc_with_optional_args(cl, good_cls)
    bad_cl1 = ['spouts', 'local/rli/defult', 'ExclamationTopology']
    return good_cls, bad_cl1

  def sample_pp_cls(self):
    clsp = ['metrics', 'local/rli/default', 'ExclamationTopology']
    clco = ['containers', 'local/rli/default', 'ExclamationTopology']
    good_cls = []
    for cl in clsp, clco:
      self.acc_with_optional_args(cl, good_cls)
    good_cls.append(clco + ['--cid 1'])
    bad_cl1 = ['metrics', 'local/rli/defult', 'ExclamationTopology']
    return good_cls, bad_cl1

  def sample_cluster_cls(self):
    cl = ['clusters']
    good_cls = []
    self.acc_with_optional_args(cl, good_cls)
    return good_cls

  def sample_cli(self):
    clt1 = ['topologies', 'local']
    clt2 = ['topologies', 'local/foo']
    clt3 = ['topologies', 'local/foo/bar']
    clb2 = ['metrics', 'local/foo/bar', 'topo']
    clc1 = ['components', 'local/foo/bar', 'topo']
    clc1 = ['components', 'local/foo/bar', 'topo', '--spout']
    clc1 = ['components', 'local/foo/bar', 'topo', '--bolt']
    clc2 = ['containers', 'local/foo/bar', 'topo']
    cll = [clt1, clt2, clt3, clb2, clc1, clc2]
    all_cl = []
    for cl in cll:
      self.acc_with_optional_args(cl, all_cl)
    # help subcommand
    for sub in ['clusters', 'topologies',
                'metrics', 'components', 'containers', 'help']:
      all_cl.append(['help', sub])
    return all_cl

  def test_topo(self):
    for cl in self.sample_topo_cls():
      self.assertEqual(0, main.main(cl))

  def test_lp(self):
    good, _ = self.sample_lp_cls()
    for cl in good:
      self.assertEqual(0, main.main(cl))
    #self.assertEqual(1, main.main(bad))

  def test_help(self):
    all_cl = []
    for sub in ['clusters', 'topologies', 'metrics',
                'components', 'containers', 'help']:
      all_cl.append(['help', sub])
    for cl in all_cl:
      self.assertEqual(0, main.main(cl))

  def test_pp(self):
    good, bad = self.sample_pp_cls()
    for cl in good:
      self.assertEqual(0, main.main(cl))
    self.assertEqual(1, main.main(bad))

  def test_sample_clusters(self):
    good = self.sample_cluster_cls()
    for cl in good:
      self.assertEqual(0, main.main(cl))

  def test_topo_result_to_table(self):
    d, told = self.sample_topo_result()
    tnew, _, _ = topologies.to_table(d)
    told.sort()
    tnew.sort()
    self.assertEqual(told, tnew)

  def test_cli_parsing(self):
    parser = main.create_parser()
    clis = self.sample_cli()
    for cli in clis:
      _, unknown_args = parser.parse_known_args(cli)
      self.assertTrue(unknown_args is not None)
