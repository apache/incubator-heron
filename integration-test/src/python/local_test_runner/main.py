import argparse
import json
import logging
import os
import pkgutil
import time
import signal
import subprocess
import sys
from collections import namedtuple
from httplib import *

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "resources/test.conf"
# Test defaults
# Test input. Please set each variable as it's own line, ended with \n, otherwise the value of lines
# passed into the topology will be incorrect, and the test will fail.
TEST_INPUT = ["1\n","2\n","3\n","4\n","5\n","6\n","7\n","8\n","9\n","10\n", "11\n", "12\n"]
TEST_CASES = [
  'KILL_TMASTER',
  'KILL_STMGR',
  'KILL_METRICSMGR',
  'KILL_STMGR_METRICSMGR'
#  'KILL_BOLT',
#  'KILL_SPOUT',
]
# Retry variables in case the output is different from the input
RETRY_COUNT = 5
RETRY_INTERVAL = 15
# Topology shard definitions
TMASTER_SHARD = 0
NON_TMASTER_SHARD = 1
# Topology process name definitions
HERON_STMGR = 'stmgr'
HERON_METRICSMGR = 'metricsmgr'
HERON_TMASTER = 'heron-tmaster'
HERON_EXECUTOR = 'heron-executor'
HERON_BOLT = 'local-write-bolt_2'
HERON_SPOUT = 'paused-local-spout_1'


ProcessTuple = namedtuple('ProcessTuple','pid cmd')

#Runs the test for one topology
def runTest(test, topologyName, params):
  #submit topology
  try:
    submitTopology(params.heronCliPath, params.heronWorkingDirectory, params.heronCorePath,
        params.testJarPath, params.topologyPath,
        topologyName, params.schedulerConfigPath, params.configLoaderClasspath)
  except Exception as e:
    logging.error("Failed to submit %s topology: %s" %(topologyName, str(e)))
    return False
  logging.info("Successfully submitted %s topology" %(topologyName))

  # block until ./heron-stmgr exists
  processList = getProcesses()
  while not processExists(processList, './heron-stmgr'):
    processList = getProcesses()

  outputFile = params.outputFile
  # insert lines into temp file and then move to read file
  try:
    with open('temp.txt', 'w') as f:
      for line in TEST_INPUT:
        f.write(line)
  except Exception as e:
    logging.error("Failed to write to temp.txt file")
    return False
  # extra time to start up, write to .pid file, connect to tmaster, etc.
  time.sleep(2)

  # execute test case
  if test == 'KILL_TMASTER':
    restartShard(params.heronCliPath, params.heronWorkingDirectory, topologyName,
        params.schedulerConfigPath, params.configLoaderClasspath, TMASTER_SHARD)
  elif test == 'KILL_STMGR':
    stmgrPid = getPid('%s-%d' % (HERON_STMGR, NON_TMASTER_SHARD), params.heronWorkingDirectory)
    killProcess(stmgrPid)
  elif test == 'KILL_METRICSMGR':
    metricsmgrPid = getPid('%s-%d' % (HERON_METRICSMGR, NON_TMASTER_SHARD), params.heronWorkingDirectory)
    killProcess(metricsmgrPid)
  elif test == 'KILL_STMGR_METRICSMGR':
    stmgrPid = getPid('%s-%d' % (HERON_STMGR, NON_TMASTER_SHARD), params.heronWorkingDirectory)
    killProcess(stmgrPid)

    metricsmgrPid = getPid('%s-%d' % (HERON_METRICSMGR, NON_TMASTER_SHARD), params.heronWorkingDirectory)
    killProcess(metricsmgrPid)
  elif test == 'KILL_BOLT':
    boltPid = getPid('container_%d_%s' % (NON_TMASTER_SHARD, HERON_BOLT), params.heronWorkingDirectory)
    killProcess(boltPid)

  # block until ./heron-stmgr exists
  processList = getProcesses()
  while not processExists(processList, './heron-stmgr'):
    processList = getProcesses()

  # move to read file. This guarantees contents will be put into the file the spout is reading from atomically
  # which increases the determinism
  os.rename('temp.txt', params.readFile)

  # sleep for 15 seconds before attempting to get results
  time.sleep(15)

  # get actual and expected result
  # retry if results are not equal a predesignated amount of times
  expectedResult = ""
  actualResult = ""
  retriesLeft = RETRY_COUNT
  while (retriesLeft > 0):
    retriesLeft -= 1
    expectedResult = ""
    actualResult = ""
    try:
      with open(params.readFile, 'r') as f:
        expectedResult = f.read()
      with open(outputFile, 'r') as g:
        actualResult = g.read()
    except Exception as e:
      logging.error("Failed to get expected and actual results")
      return False
    # if we get expected result, no need to retry
    if expectedResult == actualResult:
      break;
    if (retriesLeft > 0):
      logging.info("Failed to get proper results, retrying")
      time.sleep(RETRY_INTERVAL)

  # kill topology
  try:
    killTopology(params.heronCliPath, params.heronWorkingDirectory, topologyName, params.schedulerConfigPath, params.configLoaderClasspath)
  except Exception as e:
    logging.error("Failed to kill %s topology: %s" %(topologyName, str(e)))
    return False
  logging.info("Successfully killed %s topology" % (topologyName))
  # delete test files
  try:
    os.remove(params.readFile)
    os.remove(params.outputFile)
  except Exception as e:
    logging.error("Failed to delete test files")
    return False

  # Compare the actual and expected result
  if actualResult == expectedResult:
    logging.info("Actual result matched expected result")
    logging.info("Actual result ---------- \n" + actualResult)
    logging.info("Expected result ---------- \n" + expectedResult)
    return True
  else:
    logging.error("Actual result did not match expected result")
    logging.info("Actual result ---------- \n" + actualResult)
    logging.info("Expected result ---------- \n" + expectedResult)
    return False

# Submit topology using heron-cli
def submitTopology(heronCliPath, heronWorkingDirectory, heronCorePath, testJarPath, topologyPath, topologyName, schedulerConfigPath, configLoaderClasspath):
  logging.info("Submitting topology")
  # unicode string messes up subprocess.call quotations, must change into string type
  splitcmd = [
      '%s' % (heronCliPath),
      'submit',
      'heron.local.working.directory=%s heron.core.release.package=%s' % (heronWorkingDirectory, heronCorePath),
      '%s' % (testJarPath),
      '%s' % (topologyPath),
      '%s' % (topologyName),
      '%d' % (len(TEST_INPUT)),
      '--config-file=%s' % (schedulerConfigPath),
      '--config-loader=%s' % (configLoaderClasspath),
      '--heron-verbose']
  logging.info("Submitting topology: ")
  logging.info(splitcmd)
  subprocess.Popen(splitcmd)
  logging.info("Submitted topology")

# Kill a topology using heron-cli
def killTopology(heronCliPath, heronLocalDirectory, topologyName, schedulerConfigPath, configLoaderClasspath):
  logging.info("Killing topology")
  splitcmd = [
      '%s' % (heronCliPath),
      'kill', 'heron.local.working.directory=%s' % (heronLocalDirectory),
      '%s' % (topologyName),
      '--config-file=%s' % (schedulerConfigPath),
      '--config-loader=%s' % (configLoaderClasspath),
      '--heron-verbose']
  logging.info("Killing topology:")
  logging.info(splitcmd)
  # this call can be blocking, no need for subprocess
  if(subprocess.call(splitcmd) != 0):
    raise RuntimeError("Unable to kill the topology: %s" % topologyName)
  logging.info("Successfully killed topology")

# Run the test for each topology specified in the conf file
def runAllTests(conf, args):
  successes = []
  failures = []
  for test in TEST_CASES:
    if (runTest(test, test, args) == True): # testcase passed
      successes += [test]
    else:
      failures += [test]
  return (successes, failures)

def restartShard(heronCliPath, heronLocalDirectory, topologyName, schedulerConfigPath, configLoaderClasspath, shardNum):
  logging.info("Killing topology TMaster")
  splitcmd = [
      '%s' % (heronCliPath),
      'restart',
      'heron.local.working.directory=%s' % (heronLocalDirectory),
      '%s' % (topologyName),
      '%d' % shardNum,
      '--config-file=%s' % (schedulerConfigPath),
      '--config-loader=%s' % (configLoaderClasspath),
      '--heron-verbose']
  logging.info("Killing TMaster command:")
  logging.info(splitcmd)
  if (subprocess.call(splitcmd) != 0):
    raise RuntimeError("Unable to kill TMaster")
  logging.info("Killed tmaster")

# returns a list of process tuples (pid, cmd)
# This only applies only for local scheduler as it uses the ps command
# and assumes the topology will be running on different processes on same machine
def getProcesses():
  processes = subprocess.check_output(['ps','-o','pid,args'])
  processes = processes.split('\n')
  processes = processes[1:] # remove first line, which is name of columns
  processList = []
  for process in processes:
    # remove empty lines
    if process == '':
      continue
    pretuple = process.split(' ', 1)
    processList.append(ProcessTuple(pretuple[0],pretuple[1]))
  return processList

# opens .pid file of process and reads the first and only line, which should be the process pid
# if fail, return -1
def getPid(processName, heronWorkingDirectory):
  processPidFile = heronWorkingDirectory + processName + '.pid'
  try:
    with open(processPidFile, 'r') as f:
      pid = f.readline()
      return pid
  except Exception as e:
    logging.error("Unable to open file %s" % processPidFile)
    return -1

# kills process by running unix command kill
def killProcess(processNumber):
  logging.info("Killing process number %s" % processNumber)

  try:
    os.kill(int(processNumber), signal.SIGTERM)
  except OSError as ex:
    if ("No such process" in str(ex)): # killing a non-existing process condsidered as success
      logging.info(str(ex))
    else:
      raise RuntimeError("Unable to kill process %s" % processNumber)
  except Exception:
    raise RuntimeError("Unable to kill process %s" % processNumber)

  logging.info("Killed process number %s" % processNumber)

def processExists(processList, processCmd):
  for process in processList:
    if processCmd in process.cmd:
      return True
  return False

def main():
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)

  conf_file = DEFAULT_TEST_CONF_FILE
  # Read the configuration file from package
  confString = pkgutil.get_data(__name__, conf_file)
  decoder = json.JSONDecoder(strict=False)
  # Convert the conf file to a json format
  conf = decoder.decode(confString)

  # Get the directory of the heron root, which should be the directory that the script is run from
  heronRepoDirectory = os.getcwd() + '/'

  # Parse the arguments passed via command line
  parser = argparse.ArgumentParser(description='This is the heron integration test framework')
  parser.add_argument('-cl', '--congif-loader-classpath', dest='configLoaderClasspath', default=conf['configLoaderClasspath'])
  parser.add_argument('-hc', '--heron-cli-path', dest='heronCliPath', default=conf['heronCliPath'])
  parser.add_argument('-of', '--output-file', dest='outputFile', default=conf['heronLocalWorkingDirectory']+conf['topology']['outputFile'])
  parser.add_argument('-rf', '--read-file', dest='readFile', default=conf['heronLocalWorkingDirectory']+conf['topology']['readFile'])
  parser.add_argument('-rp', '--heron-core-path', dest='heronCorePath', default=heronRepoDirectory+conf['heronCorePath'])
  parser.add_argument('-sc', '--scheduler-config-path', dest='schedulerConfigPath', default=heronRepoDirectory+conf['schedulerConfigPath'])
  parser.add_argument('-tj', '--test-jar-path', dest='testJarPath', default=heronRepoDirectory+conf['testJarPath'])
  parser.add_argument('-tn', '--topology-name', dest='topologyName', default=conf['topology']['topologyName'])
  parser.add_argument('-tp', '--topology-path', dest='topologyPath', default=conf['topology']['topologyClasspath'])
  parser.add_argument('-wd', '--heron-working-directory', dest='heronWorkingDirectory', default=heronRepoDirectory+conf['heronLocalWorkingDirectory'])
  args = parser.parse_args()
  (successes, failures) = runAllTests(conf, args)

  if not failures:
    logging.info("Success: %s (all) tests passed" % len(successes))
    sys.exit(0)
  else:
    logging.error("Fail: %s test failed" %len(failures))
    logging.info("Failed Tests: ")
    logging.info("\n".join(failures))
    sys.exit(1)

if __name__ == '__main__':
  main()
