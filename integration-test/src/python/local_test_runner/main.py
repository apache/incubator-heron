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
#!/usr/bin/env python2.7

""" main.py """
import getpass
import json
import logging
import os
import pkgutil
import time
import socket
import subprocess
import sys
from collections import namedtuple

# import test_kill_bolt
import test_kill_metricsmgr
import test_kill_stmgr
import test_kill_stmgr_metricsmgr
import test_kill_tmaster
import test_scale_up

TEST_CLASSES = [
    test_kill_tmaster.TestKillTMaster,
    test_kill_stmgr.TestKillStmgr,
    test_kill_metricsmgr.TestKillMetricsMgr,
    test_kill_stmgr_metricsmgr.TestKillStmgrMetricsMgr,
    test_scale_up.TestScaleUp,
    # test_kill_bolt.TestKillBolt,
]

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "resources/test.conf"

ProcessTuple = namedtuple('ProcessTuple', 'pid cmd')

def run_all_tests(args):
  """ Run the test for each topology specified in the conf file """
  successes = []
  failures = []
  tracker_process = _start_tracker(args['trackerPath'], args['trackerPort'])

  try:
    for testclass in TEST_CLASSES:
      testname = testclass.__name__
      logging.info("==== Starting test %s of %s: %s ====",
                   len(successes) + len(failures) + 1, len(TEST_CLASSES), testname)
      template = testclass(testname, args)
      if template.run_test(): # testcase passed
        successes += [testname]
      else:
        failures += [testname]
  except Exception as e:
    logging.error("Exception thrown while running tests: %s", str(e))
  finally:
    tracker_process.kill()

  return successes, failures

def _start_tracker(tracker_path, tracker_port):
  splitcmd = [tracker_path, '--verbose', '--port=%s' % tracker_port]

  logging.info("Starting heron tracker: %s", splitcmd)
  popen = subprocess.Popen(splitcmd)
  logging.info("Successfully started heron tracker on port %s", tracker_port)
  return popen

def _random_port():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(("", 0))
  s.listen(1)
  port = s.getsockname()[1]
  s.close()
  return port

def main():
  """ main """
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)

  # Read the configuration file from package
  conf_file = DEFAULT_TEST_CONF_FILE
  conf_string = pkgutil.get_data(__name__, conf_file)
  decoder = json.JSONDecoder(strict=False)

  # Convert the conf file to a json format
  conf = decoder.decode(conf_string)

  # Get the directory of the heron root, which should be the directory that the script is run from
  heron_repo_directory = os.getcwd()

  args = dict()
  home_directory = os.path.expanduser("~")
  args['cluster'] = conf['cluster']
  args['topologyName'] = conf['topology']['topologyName']
  args['topologyClassPath'] = conf['topology']['topologyClassPath']
  args['workingDirectory'] = os.path.join(
      home_directory,
      ".herondata",
      "topologies",
      conf['cluster'],
      getpass.getuser(),
      args['topologyName']
  )
  args['cliPath'] = os.path.expanduser(conf['heronCliPath'])
  args['trackerPath'] = os.path.expanduser(conf['heronTrackerPath'])
  args['trackerPort'] = _random_port()
  args['outputFile'] = os.path.join(args['workingDirectory'], conf['topology']['outputFile'])
  args['readFile'] = os.path.join(args['workingDirectory'], conf['topology']['readFile'])
  args['testJarPath'] = os.path.join(heron_repo_directory, conf['testJarPath'])

  start_time = time.time()
  (successes, failures) = run_all_tests(args)
  elapsed_time = time.time() - start_time
  total = len(failures) + len(successes)

  if not failures:
    logging.info("Success: %s (all) tests passed", len(successes))
    logging.info("Elapsed time: %s", elapsed_time)
    sys.exit(0)
  else:
    logging.error("Fail: %s/%s test failed:", len(failures), total)
    for test in failures:
      logging.error("  - %s", test)
    sys.exit(1)

if __name__ == '__main__':
  main()
