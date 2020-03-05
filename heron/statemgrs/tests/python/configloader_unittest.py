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

'''heron executor unittest'''
import os
import unittest2 as unittest

from heron.statemgrs.src.python import configloader

class ConfigLoaderTest(unittest.TestCase):
  """Unittest for ConfigLoader"""

  def setUp(self):
    os.environ['HOME'] = '/user/fake'

    # Find the local working dir. For example, __file__ would be
    # /tmp/_bazel_heron/randgen_dir/heron/heron/statemgrs/tests/python/configloader_unittest.py
    self.heron_dir = '/'.join(__file__.split('/')[:-5])

  def load_locations(self, cluster):
    yaml_path = os.path.join(self.heron_dir,
                             'heron/config/src/yaml/conf/%s/statemgr.yaml' % cluster)
    return configloader.load_state_manager_locations(cluster, yaml_path)

  def test_load_state_manager_locations_aurora(self):
    self.assertEqual([{
      'hostport': 'LOCALMODE',
      'name': 'local',
      'rootpath': '/vagrant/.herondata/repository/state/aurora',
      'tunnelhost': 'my.tunnel.host',
      'type': 'file'
    }], self.load_locations('aurora'))

  def test_load_state_manager_locations_local(self):
    self.assertEqual([{
      'hostport': 'LOCALMODE',
      'name': 'local',
      'rootpath': '/user/fake/.herondata/repository/state/local',
      'tunnelhost': '127.0.0.1',
      'type': 'file'
    }], self.load_locations('local'))

  def test_load_state_manager_locations_localzk(self):
    self.assertEqual([{
      'hostport': '127.0.0.1:2181',
      'name': 'zk',
      'rootpath': '/heron',
      'tunnelhost': 'reachable.tunnel.host',
      'type': 'zookeeper'
    }], self.load_locations('localzk'))

  def test_load_state_manager_locations_marathon(self):
    self.assertEqual([{
      'hostport': 'LOCALMODE',
      'name': 'local',
      'rootpath': '/user/fake/.herondata/repository/state/marathon',
      'tunnelhost': '127.0.0.1',
      'type': 'file'
    }], self.load_locations('marathon'))

  def test_load_state_manager_locations_mesos(self):
    self.assertEqual([{
      'hostport': 'LOCALMODE',
      'name': 'local',
      'rootpath': '/user/fake/.herondata/repository/state/mesos',
      'tunnelhost': '127.0.0.1',
      'type': 'file'
    }], self.load_locations('mesos'))

  def test_load_state_manager_locations_slurm(self):
    self.assertEqual([{
      'hostport': 'LOCALMODE',
      'name': 'local',
      'rootpath': '/user/fake/.herondata/repository/state/slurm',
      'tunnelhost': '127.0.0.1',
      'type': 'file'
    }], self.load_locations('slurm'))

  def test_load_state_manager_locations_yarn(self):
    self.assertEqual([{
      'hostport': 'LOCALMODE',
      'name': 'local',
      'rootpath': '/user/fake/.herondata/repository/state/yarn',
      'tunnelhost': '127.0.0.1',
      'type': 'file'
    }], self.load_locations('yarn'))
