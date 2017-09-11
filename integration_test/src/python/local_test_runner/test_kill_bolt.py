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

"""test_kill_bolt.py"""
import logging
import test_template

NON_TMASTER_SHARD = 1
HERON_BOLT = 'identity-bolt_3'

class TestKillBolt(test_template.TestTemplate):

  def execute_test_case(self):
    logging.info("Executing kill bolt")
    bolt_pid = self.get_pid(
        'container_%d_%s' % (NON_TMASTER_SHARD, HERON_BOLT), self.params['workingDirectory'])
    self.kill_process(bolt_pid)
