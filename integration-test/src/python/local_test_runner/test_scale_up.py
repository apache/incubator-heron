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

"""test_scale_up.py"""
import json
import logging
import subprocess
import urllib

import test_template

class TestScaleUp(test_template.TestTemplate):

  def execute_test_case(self):
    scale_up(self.params['cliPath'], self.params['cluster'], self.params['topologyName'])

  def pre_check_results(self):
    url = 'http://localhost:%s/topologies/physicalplan?' % self.params['trackerPort']\
          + 'cluster=local&environ=default&topology=IntegrationTest_LocalReadWriteTopology'
    response = urllib.urlopen(url)
    physical_plan_json = json.loads(response.read())
    expected_instance_count = 5
    def assert_scaling():
      if 'result' not in physical_plan_json:
        logging.error("Could not find result json in physical plan request to tracker: %s", url)
        return False

      instances = physical_plan_json['result']['instances']
      instance_count = len(instances)
      if instance_count != expected_instance_count:
        logging.error("Found %s instances but expected %s: %s",
                      instance_count, expected_instance_count, instances)
        return False

      return True

    scaling_asserted = assert_scaling()
    if not scaling_asserted:
      self.cleanup_test()

    return scaling_asserted

def scale_up(heron_cli_path, test_cluster, topology_name):
  splitcmd = [
      heron_cli_path, 'update', '--verbose', test_cluster, topology_name,
      '--component-parallelism=paused-local-spout:2',
      '--component-parallelism=identity-bolt:2'
  ]
  logging.info("Increasing number of component instances: %s", splitcmd)
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to update topology")
  logging.info("Increased number of component instances")
