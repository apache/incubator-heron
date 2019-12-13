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

""" test_template.py """
from future.standard_library import install_aliases
install_aliases()

import json
import logging
import os
import time
from urllib.request import urlopen
import signal
import subprocess
from collections import namedtuple

from ..common import status

# Test input. Please set each variable as it's own line, ended with \n, otherwise the value of lines
# passed into the topology will be incorrect, and the test will fail.
TEST_INPUT = ["1\n", "2\n", "3\n", "4\n", "5\n", "6\n", "7\n", "8\n",
              "9\n", "10\n", "11\n", "12\n"]

# Retry variables in case the output is different from the input
RETRY_COUNT = 5
RETRY_INTERVAL = 10
# Topology shard definitions
NON_TMASTER_SHARD = 1
# Topology process name definitions
STMGR = 'stmgr'
HERON_BIN = "bin"
HERON_CORE = "heron-core"
HERON_METRICSMGR = 'metricsmgr'
HERON_SANDBOX_HOME = "."
HERON_STMGR = "heron-stmgr"
HERON_STMGR_CMD = os.path.join(HERON_SANDBOX_HOME, HERON_CORE, HERON_BIN, HERON_STMGR)
ProcessTuple = namedtuple('ProcessTuple', 'pid cmd')

class TestTemplate(object):
  """ Class that encapsulates the template used for integration tests. Intended to be abstract and
  subclassed for specific tests. """

  def __init__(self, testname, params):
    self.testname = testname
    self.params = params

  # pylint: disable=too-many-return-statements, too-many-branches,
  # pylint: disable=too-many-statements
  def run_test(self):
    """ Runs the test template. Must either return TestSuccess or raise TestFailure"""
    topology_submitted = False
    try:
      # prepare test data, start the topology and block until it's running
      self._prepare_test_data()
      self.submit_topology()
      topology_submitted = True
      _block_until_stmgr_running(self.get_expected_container_count())

      self._block_until_topology_running(self.get_expected_min_instance_count())

      # Execute the specific test logic and block until topology is running again
      self.execute_test_case()

      _block_until_stmgr_running(self.get_expected_container_count())
      physical_plan_json =\
        self._block_until_topology_running(self.get_expected_min_instance_count())

      # trigger the test data to flow and invoke the pre_check_results hook
      self._inject_test_data()
      self.pre_check_results(physical_plan_json)

      # finally verify the expected results
      result = self._check_results()
      return result

    except status.TestFailure as e:
      raise e
    except Exception as e:
      raise status.TestFailure("Exception thrown during test", e)
    finally:
      if topology_submitted:
        self.cleanup_test()

  def submit_topology(self):
    _submit_topology(
        self.params['cliPath'],
        self.params['cluster'],
        self.params['testJarPath'],
        self.params['topologyClassPath'],
        self.params['topologyName'],
        self.params['readFile'],
        self.params['outputFile']
    )

  # pylint: disable=no-self-use
  def get_expected_container_count(self):
    return 1

  # pylint: disable=no-self-use
  def get_expected_min_instance_count(self):
    return 1

  def execute_test_case(self):
    pass

  # pylint: disable=no-self-use,unused-argument
  def pre_check_results(self, physical_plan_json):
    return True

  def cleanup_test(self):
    try:
      _kill_topology(self.params['cliPath'], self.params['cluster'], self.params['topologyName'])
    except Exception as e:
      logging.error("Failed to kill %s topology: %s", self.params['topologyName'], str(e))
    finally:
      self._delete_test_data_files()

  def _delete_test_data_files(self):
    _safe_delete_file(self.params['readFile'])
    _safe_delete_file(self.params['outputFile'])

  def _prepare_test_data(self):
    self._delete_test_data_files()

    # insert lines into temp file and then move to read file
    try:
      with open('temp.txt', 'w') as f:
        for line in TEST_INPUT:
          f.write(line)
    except Exception as e:
      logging.error("Failed to write to temp.txt file: %s", str(e))
      return False

  def _inject_test_data(self):
    # move to read file. This guarantees contents will be put into the file the
    # spout is reading from atomically
    # which increases the determinism
    os.rename('temp.txt', self.params['readFile'])

  def _check_results(self):
    """ get actual and expected result.
    retry if results are not equal a predesignated amount of times
    """
    expected_result = ""
    actual_result = ""
    retries_left = RETRY_COUNT
    _sleep("before trying to check results for test %s" % self.testname, RETRY_INTERVAL)
    while retries_left > 0:
      retries_left -= 1
      try:
        with open(self.params['readFile'], 'r') as f:
          expected_result = f.read()
        with open(self.params['outputFile'], 'r') as g:
          actual_result = g.read()
      except Exception as e:
        message =\
          "Failed to read expected or actual results from file for test %s: %s" % self.testname
        if retries_left == 0:
          raise status.TestFailure(message, e)
        logging.error(message, e)
      # if we get expected result, no need to retry
      expected_sorted = sorted(expected_result.split('\n'))
      actual_sorted = sorted(actual_result.split('\n'))
      if expected_sorted == actual_sorted:
        break
      if retries_left > 0:
        expected_result = ""
        actual_result = ""
        expected_sorted = []
        actual_sorted = []
        logging.info("Failed to get expected results for test %s (attempt %s/%s), "\
                     + "retrying after %s seconds",
                     self.testname, RETRY_COUNT - retries_left, RETRY_COUNT, RETRY_INTERVAL)
        time.sleep(RETRY_INTERVAL)

    # Compare the actual and expected result
    if actual_sorted == expected_sorted:
      success = status.TestSuccess(
          "Actual result matched expected result for test %s" % self.testname)
      logging.info("Actual result ---------- \n%s", actual_sorted)
      logging.info("Expected result ---------- \n%s", expected_sorted)
      return success
    else:
      failure = status.TestFailure(
          "Actual result did not match expected result for test %s" % self.testname)
      logging.info("Actual result ---------- \n%s", actual_sorted)
      logging.info("Expected result ---------- \n%s", expected_sorted)
      raise failure

  # pylint: disable=no-self-use
  def get_pid(self, process_name, heron_working_directory):
    """
    opens .pid file of process and reads the first and only line, which should be the process pid
    if fail, return -1
    """
    process_pid_file = os.path.join(heron_working_directory, process_name + '.pid')
    try:
      with open(process_pid_file, 'r') as f:
        pid = f.readline()
        return pid
    except Exception:
      logging.error("Unable to open file %s", process_pid_file)
      return -1

  # pylint: disable=no-self-use
  def kill_process(self, process_number):
    """ kills process by running unix command kill """
    if process_number < 1:
      raise RuntimeError(
          "Not attempting to kill process id < 1 passed to kill_process: %d" % process_number)

    logging.info("Killing process number %s", process_number)

    try:
      os.kill(int(process_number), signal.SIGTERM)
    except OSError as ex:
      if "No such process" in str(ex): # killing a non-existing process condsidered as success
        logging.info(str(ex))
      else:
        raise RuntimeError("Unable to kill process %s" % process_number)
    except Exception:
      raise RuntimeError("Unable to kill process %s" % process_number)

    logging.info("Killed process number %s", process_number)

  def kill_strmgr(self):
    logging.info("Executing kill stream manager")
    stmgr_pid = self.get_pid('%s-%d' % (STMGR, NON_TMASTER_SHARD), self.params['workingDirectory'])
    self.kill_process(stmgr_pid)

  def kill_metricsmgr(self):
    logging.info("Executing kill metrics manager")
    metricsmgr_pid = self.get_pid(
        '%s-%d' % (HERON_METRICSMGR, NON_TMASTER_SHARD), self.params['workingDirectory'])
    self.kill_process(metricsmgr_pid)

  def _get_tracker_pplan(self):
    url = 'http://localhost:%s/topologies/physicalplan?' % self.params['trackerPort']\
          + 'cluster=local&environ=default&topology=IntegrationTest_LocalReadWriteTopology'
    logging.debug("Fetching physical plan from %s", url)
    response = urlopen(url)
    physical_plan_json = json.loads(response.read())

    if 'result' not in physical_plan_json:
      raise status.TestFailure(
          "Could not find result json in physical plan request to tracker: %s" % url)

    return physical_plan_json['result']

  def _block_until_topology_running(self, min_instances):
    retries_left = RETRY_COUNT
    _sleep("before trying to fetch pplan for test %s" % self.testname, RETRY_INTERVAL)
    while retries_left > 0:
      retries_left -= 1
      packing_plan = self._get_tracker_pplan()
      if packing_plan:
        instances_found = len(packing_plan['instances'])
        if instances_found >= min_instances:
          logging.info("Successfully fetched pplan from tracker for test %s after %s attempts.",
                       self.testname, RETRY_COUNT - retries_left)
          return packing_plan
        elif retries_left == 0:
          raise status.TestFailure(
              "Got pplan from tracker for test %s but the number of " % self.testname +
              "instances found (%d) was less than min expected (%s)." %
              (instances_found, min_instances))

      if retries_left > 0:
        _sleep("before trying again to fetch pplan for test %s (attempt %s/%s)" %
               (self.testname, RETRY_COUNT - retries_left, RETRY_COUNT), RETRY_INTERVAL)
      else:
        raise status.TestFailure("Failed to get pplan from tracker for test %s after %s attempts."
                                 % (self.testname, RETRY_COUNT))

def _block_until_stmgr_running(expected_stmgrs):
  # block until ./heron-stmgr exists
  process_list = _get_processes()
  while not _processes_exists(process_list, HERON_STMGR_CMD, expected_stmgrs):
    process_list = _get_processes()
    time.sleep(1)

def _submit_topology(heron_cli_path, test_cluster, test_jar_path, topology_class_path,
                     topology_name, input_file, output_file):
  """ Submit topology using heron-cli """
  # unicode string messes up subprocess.call quotations, must change into string type
  splitcmd = [
      heron_cli_path, 'submit', '--verbose', '--', test_cluster, test_jar_path,
      topology_class_path, topology_name, input_file, output_file, str(len(TEST_INPUT))
  ]
  logging.info("Submitting topology: %s", splitcmd)
  p = subprocess.Popen(splitcmd)
  p.wait()
  if p.returncode != 0:
    raise status.TestFailure("Failed to submit topology %s" % topology_name)

  logging.info("Submitted topology %s", topology_name)

def _kill_topology(heron_cli_path, test_cluster, topology_name):
  """ Kill a topology using heron-cli """
  splitcmd = [heron_cli_path, 'kill', test_cluster, topology_name]
  logging.info("Killing topology: %s", ' '.join(splitcmd))
  # this call can be blocking, no need for subprocess
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to kill the topology: %s" % topology_name)

def _get_processes():
  """
  returns a list of process tuples (pid, cmd)
  This only applies only for local scheduler as it uses the ps command
  and assumes the topology will be running on different processes on same machine
  """
  # pylint: disable=fixme
  # TODO: if the submit fails before we get here (e.g., Topology already exists), this hangs
  processes = subprocess.check_output(['ps', '-o', 'pid,args'])
  processes = processes.split('\n')
  processes = processes[1:] # remove first line, which is name of columns
  process_list = []
  for process in processes:
    # remove empty lines
    if process == '':
      continue
    pretuple = process.split(' ', 1)
    process_list.append(ProcessTuple(pretuple[0], pretuple[1]))
  return process_list

def _sleep(message, seconds):
  logging.info("Sleeping for %d seconds %s", seconds, message)
  time.sleep(seconds)

def _processes_exists(process_list, process_cmd, min_processes):
  """ check if a process is running """
  proccess_count = 0
  for process in process_list:
    if process_cmd in process.cmd:
      proccess_count += 1

  return proccess_count >= min_processes

def _safe_delete_file(file_name):
  if os.path.isfile(file_name) and os.path.exists(file_name):
    try:
      os.remove(file_name)
    except Exception as e:
      logging.error("Failed to delete file: %s: %s", file_name, e)
      return False
