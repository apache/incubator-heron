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


"""test_kill_tmanager.py"""
import logging
import subprocess
from . import test_template

TMANAGER_SHARD = 0

class TestKillTManager(test_template.TestTemplate):

  def execute_test_case(self):
    restart_shard(self.params['cliPath'], self.params['cluster'],
                  self.params['topologyName'], TMANAGER_SHARD)

def restart_shard(heron_cli_path, test_cluster, topology_name, shard_num):
  """ restart tmanager """
  splitcmd = [heron_cli_path, 'restart', '--verbose', test_cluster, topology_name, str(shard_num)]

  logging.info("Killing TManager: %s", splitcmd)
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to kill TManager")
  logging.info("Killed TManager")
