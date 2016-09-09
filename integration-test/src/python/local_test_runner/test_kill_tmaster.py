# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""test_kill_tmaster.py"""
import logging
import subprocess
import test_template

TMASTER_SHARD = 0

class TestKillTMaster(test_template.TestTemplate):

  def execute_test_case(self):
    restart_shard(self.params['cliPath'], self.params['cluster'],
                  self.params['topologyName'], TMASTER_SHARD)

def restart_shard(heron_cli_path, test_cluster, topology_name, shard_num):
  """ restart tmaster """
  splitcmd = [heron_cli_path, 'restart', '--verbose', test_cluster, topology_name, str(shard_num)]

  logging.info("Killing TMaster: %s", splitcmd)
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to kill TMaster")
  logging.info("Killed TMaster")
