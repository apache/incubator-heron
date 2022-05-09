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

from ..common import status
from heron.common.src.python.utils import log

# import test_kill_bolt
from . import test_kill_metricsmgr
from . import test_kill_stmgr
from . import test_kill_stmgr_metricsmgr
from . import test_kill_tmanager
from . import test_scale_up
from . import test_template
from . import test_explorer

TEST_CLASSES = [
    test_template.TestTemplate,
    test_kill_tmanager.TestKillTManager,
    test_kill_stmgr.TestKillStmgr,
    test_kill_metricsmgr.TestKillMetricsMgr,
    test_kill_stmgr_metricsmgr.TestKillStmgrMetricsMgr,
    test_scale_up.TestScaleUp,
    # test_kill_bolt.TestKillBolt,
    test_explorer.TestExplorer,
]

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "resources/test.conf"

ProcessTuple = namedtuple('ProcessTuple', 'pid cmd')

def run_tests(test_classes, args):
  """ Run the test for each topology specified in the conf file """
  successes = []
  failures = []
  tracker_process = _start_tracker(args['trackerPath'], args['trackerPort'])

  try:
    for test_class in test_classes:
      testname = test_class.__name__
      logging.info("==== Starting test %s of %s: %s ====",
                   len(successes) + len(failures) + 1, len(test_classes), testname)
      template = test_class(testname, args)
      try:
        result = template.run_test()
        if isinstance(result, status.TestSuccess): # testcase passed
          successes += [testname]
        elif isinstance(result, status.TestFailure):
          failures += [testname]
        else:
          logging.error(
              "Unrecognized test response returned for test %s: %s", testname, str(result))
          failures += [testname]
      except status.TestFailure:
        failures += [testname]

  except Exception as e:
    logging.error("Exception thrown while running tests: %s", str(e), exc_info=True)
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
  log.configure(level=logging.DEBUG)

  # Read the configuration file from package
  conf_file = DEFAULT_TEST_CONF_FILE
  conf_string = pkgutil.get_data(__name__, conf_file).decode()
  decoder = json.JSONDecoder(strict=False)

  # Convert the conf file to a json format
  conf = decoder.decode(conf_string)

  args = {}
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
  args['testJarPath'] = conf['testJarPath']

  test_classes = TEST_CLASSES
  if len(sys.argv) > 1:
    first_arg = sys.argv[1]
    class_tokens = first_arg.split(".")
    if first_arg == "-h" or len(class_tokens) < 2:
      usage()

    import importlib
    package_tokens = class_tokens[:-1]
    test_class = class_tokens[-1]
    if len(package_tokens) == 1: # prepend base packages for convenience
      test_module = "integration_test.src.python.local_test_runner." + package_tokens[0]
    else:
      test_module = '.'.join(package_tokens)

    logging.info("test_module %s", test_module)
    logging.info("test_class %s", test_class)
    test_classes = [getattr(importlib.import_module(test_module), test_class)]

  start_time = time.time()
  (successes, failures) = run_tests(test_classes, args)
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

def usage():
  logging.info("Usage: python %s [<test_module>.<testname>]", sys.argv[0])
  sys.exit(1)

if __name__ == '__main__':
  main()
