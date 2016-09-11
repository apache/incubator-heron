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
""" test_template.py """
import logging
import os
import time
import signal
import subprocess
from collections import namedtuple

# Test input. Please set each variable as it's own line, ended with \n, otherwise the value of lines
# passed into the topology will be incorrect, and the test will fail.
TEST_INPUT = ["1\n", "2\n", "3\n", "4\n", "5\n", "6\n", "7\n", "8\n",
              "9\n", "10\n", "11\n", "12\n"]

# Retry variables in case the output is different from the input
RETRY_COUNT = 5
RETRY_INTERVAL = 30
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
    """ Runs the test template """

    try:
      self.submit_topology()
      _block_until_topology_running()

      self._prepare_test_data()

      _sleep("to allow time for startup", 30)
      self.execute_test_case()
      _block_until_topology_running()

      self._inject_test_data()
      _sleep("before checking for results", 30)

      if not self.pre_check_results():
        self.cleanup_test()
        return False
    except Exception as e:
      logging.error("Test failed, attempting to clean up: %s", e)
      self.cleanup_test()
      return False

    return self._check_results()

  def submit_topology(self):
    #submit topology
    try:
      _submit_topology(
          self.params['cliPath'],
          self.params['cluster'],
          self.params['testJarPath'],
          self.params['topologyClassPath'],
          self.params['topologyName'],
          self.params['readFile'],
          self.params['outputFile']
      )
    except Exception as e:
      logging.error("Failed to submit %s topology: %s", self.params['topologyName'], str(e))
      return False

  def execute_test_case(self):
    pass

  # pylint: disable=no-self-use
  def pre_check_results(self):
    return True

  def cleanup_test(self):
    try:
      _kill_topology(self.params['cliPath'], self.params['cluster'], self.params['topologyName'])
    except Exception as e:
      logging.error("Failed to kill %s topology: %s", self.params['topologyName'], str(e))
      return False
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
    while retries_left > 0:
      retries_left -= 1
      try:
        with open(self.params['readFile'], 'r') as f:
          expected_result = f.read()
        with open(self.params['outputFile'], 'r') as g:
          actual_result = g.read()
      except Exception as e:
        logging.error(
            "Failed to read expected or actual results from file for test %s: %s", self.testname, e)
        self.cleanup_test()
        return False
      # if we get expected result, no need to retry
      if expected_result == actual_result:
        break
      if retries_left > 0:
        expected_result = ""
        actual_result = ""
        logging.info("Failed to get expected results for test %s (attempt %s/%s), "\
                     + "retrying after %s seconds",
                     self.testname, RETRY_COUNT - retries_left, RETRY_COUNT, RETRY_INTERVAL)
        time.sleep(RETRY_INTERVAL)

    self.cleanup_test()

    # Compare the actual and expected result
    if actual_result == expected_result:
      logging.info("Actual result matched expected result for test %s", self.testname)
      logging.info("Actual result ---------- \n" + actual_result)
      logging.info("Expected result ---------- \n" + expected_result)
      return True
    else:
      logging.error("Actual result did not match expected result for test %s", self.testname)
      logging.info("Actual result ---------- \n" + actual_result)
      logging.info("Expected result ---------- \n" + expected_result)
      return False

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

def _block_until_topology_running():
  # block until ./heron-stmgr exists
  process_list = _get_processes()
  while not _process_exists(process_list, HERON_STMGR_CMD):
    process_list = _get_processes()

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
  logging.info("Submitted topology %s", topology_name)

def _kill_topology(heron_cli_path, test_cluster, topology_name):
  """ Kill a topology using heron-cli """
  splitcmd = [heron_cli_path, 'kill', '--verbose', test_cluster, topology_name]
  logging.info("Killing topology: %s", splitcmd)
  # this call can be blocking, no need for subprocess
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to kill the topology: %s" % topology_name)
  logging.info("Successfully killed topology %s", topology_name)

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

def _process_exists(process_list, process_cmd):
  """ check if a process is running """
  for process in process_list:
    if process_cmd in process.cmd:
      return True
  return False

def _safe_delete_file(file_name):
  if os.path.isfile(file_name) and os.path.exists(file_name):
    try:
      os.remove(file_name)
    except Exception as e:
      logging.error("Failed to delete file: %s: %s", file_name, e)
      return False
