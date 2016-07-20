''' main.py '''
import getpass
import json
import logging
import os
import pkgutil
import time
import signal
import subprocess
import sys
from collections import namedtuple

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "resources/test.conf"
# Test defaults
# Test input. Please set each variable as it's own line, ended with \n, otherwise the value of lines
# passed into the topology will be incorrect, and the test will fail.
TEST_INPUT = ["1\n", "2\n", "3\n", "4\n", "5\n", "6\n", "7\n", "8\n",
              "9\n", "10\n", "11\n", "12\n"]
TEST_CASES = [
    'KILL_TMASTER',
    'KILL_STMGR',
    'KILL_METRICSMGR',
    'KILL_STMGR_METRICSMGR'
]
# Retry variables in case the output is different from the input
RETRY_COUNT = 5
RETRY_INTERVAL = 60
# Topology shard definitions
TMASTER_SHARD = 0
NON_TMASTER_SHARD = 1
# Topology process name definitions
STMGR = 'stmgr'
HERON_BIN = "bin"
HERON_BOLT = 'local-write-bolt_2'
HERON_CORE = "heron-core"
HERON_EXECUTOR = 'heron-executor'
HERON_METRICSMGR = 'metricsmgr'
HERON_SANDBOX_HOME = "."
HERON_SPOUT = 'paused-local-spout_1'
HERON_STMGR = "heron-stmgr"
HERON_STMGR_CMD = os.path.join(HERON_SANDBOX_HOME, HERON_CORE, HERON_BIN, HERON_STMGR)
HERON_TMASTER = 'heron-tmaster'

ProcessTuple = namedtuple('ProcessTuple', 'pid cmd')

# pylint: disable=too-many-return-statements, too-many-branches,
# pylint: disable=too-many-statements
def runTest(test, topologyName, params):
  ''' Runs the test for one topology '''
  #submit topology
  try:
    submitTopology(
        params['cliPath'],
        params['cluster'],
        params['testJarPath'],
        params['topologyClassPath'],
        params['topologyName'],
        params['readFile'],
        params['outputFile']
    )
  except Exception as e:
    logging.error("Failed to submit %s topology: %s", topologyName, str(e))
    return False
  logging.info("Successfully submitted %s topology", topologyName)

  # block until ./heron-stmgr exists
  processList = getProcesses()
  while not processExists(processList, HERON_STMGR_CMD):
    processList = getProcesses()

  # insert lines into temp file and then move to read file
  try:
    with open('temp.txt', 'w') as f:
      for line in TEST_INPUT:
        f.write(line)
  except Exception as e:
    logging.error("Failed to write to temp.txt file")
    return False
  # extra time to start up, write to .pid file, connect to tmaster, etc.
  time.sleep(30)

  # execute test case
  if test == 'KILL_TMASTER':
    print "Executing kill tmaster"
    restartShard(params['cliPath'], params['cluster'], params['topologyName'], TMASTER_SHARD)
  elif test == 'KILL_STMGR':
    print "Executing kill stmgr"
    stmgrPid = getPid('%s-%d' % (STMGR, NON_TMASTER_SHARD), params['workingDirectory'])
    killProcess(stmgrPid)
  elif test == 'KILL_METRICSMGR':
    print "Executing kill metrics manager"
    metricsmgrPid = getPid('%s-%d' % (HERON_METRICSMGR, NON_TMASTER_SHARD),
                           params['workingDirectory'])
    killProcess(metricsmgrPid)
  elif test == 'KILL_STMGR_METRICSMGR':
    print "Executing kill stmgr metrics manager"
    stmgrPid = getPid('%s-%d' % (STMGR, NON_TMASTER_SHARD), params['workingDirectory'])
    killProcess(stmgrPid)

    metricsmgrPid = getPid('%s-%d' % (HERON_METRICSMGR, NON_TMASTER_SHARD),
                           params['workingDirectory'])
    killProcess(metricsmgrPid)
  elif test == 'KILL_BOLT':
    print "Executing kill bolt"
    boltPid = getPid('container_%d_%s' % (NON_TMASTER_SHARD, HERON_BOLT),
                     params['workingDirectory'])
    killProcess(boltPid)

  # block until ./heron-stmgr exists
  processList = getProcesses()
  while not processExists(processList, HERON_STMGR_CMD):
    processList = getProcesses()

  # move to read file. This guarantees contents will be put into the file the
  # spout is reading from atomically
  # which increases the determinism
  os.rename('temp.txt', params['readFile'])

  # sleep for 15 seconds before attempting to get results
  time.sleep(15)

  # get actual and expected result
  # retry if results are not equal a predesignated amount of times
  expectedResult = ""
  actualResult = ""
  retriesLeft = RETRY_COUNT
  while retriesLeft > 0:
    retriesLeft -= 1
    expectedResult = ""
    actualResult = ""
    try:
      with open(params['readFile'], 'r') as f:
        expectedResult = f.read()
      with open(params['outputFile'], 'r') as g:
        actualResult = g.read()
    except Exception as e:
      logging.error("Failed to get expected and actual results")
      return False
    # if we get expected result, no need to retry
    if expectedResult == actualResult:
      break
    if retriesLeft > 0:
      logging.info("Failed to get proper results, retrying")
      time.sleep(RETRY_INTERVAL)

  # kill topology
  try:
    killTopology(params['cliPath'], params['cluster'], params['topologyName'])
  except Exception as e:
    logging.error("Failed to kill %s topology: %s", topologyName, str(e))
    return False
  logging.info("Successfully killed %s topology", topologyName)

  # delete test files
  try:
    os.remove(params['readFile'])
    os.remove(params['outputFile'])
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

def submitTopology(heronCliPath, testCluster, testJarPath, topologyClassPath,
                   topologyName, inputFile, outputFile):
  ''' Submit topology using heron-cli '''
  logging.info("Submitting topology")
  # unicode string messes up subprocess.call quotations, must change into string type
  splitcmd = [
      '%s' % (heronCliPath),
      'submit',
      '--verbose',
      '--',
      '%s' % (testCluster),
      '%s' % (testJarPath),
      '%s' % (topologyClassPath),
      '%s' % (topologyName),
      '%s' % (inputFile),
      '%s' % (outputFile),
      '%d' % (len(TEST_INPUT))
  ]
  logging.info("Submitting topology: ")
  logging.info(splitcmd)
  p = subprocess.Popen(splitcmd)
  p.wait()
  logging.info("Submitted topology")

def killTopology(heronCliPath, testCluster, topologyName):
  ''' Kill a topology using heron-cli '''
  logging.info("Killing topology")
  splitcmd = [
      '%s' % (heronCliPath),
      'kill',
      '--verbose',
      '%s' % (testCluster),
      '%s' % (topologyName),
  ]
  logging.info("Killing topology:")
  logging.info(splitcmd)
  # this call can be blocking, no need for subprocess
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to kill the topology: %s" % topologyName)
  logging.info("Successfully killed topology")

def runAllTests(args):
  ''' Run the test for each topology specified in the conf file '''
  successes = []
  failures = []
  for test in TEST_CASES:
    if runTest(test, test, args): # testcase passed
      successes += [test]
    else:
      failures += [test]
  return (successes, failures)

def restartShard(heronCliPath, testCluster, topologyName, shardNum):
  ''' restart tmaster '''
  logging.info("Killing topology TMaster")
  splitcmd = [
      '%s' % (heronCliPath),
      'restart',
      '--verbose',
      '%s' % (testCluster),
      '%s' % (topologyName),
      '%d' % shardNum
  ]
  logging.info("Killing TMaster command:")
  logging.info(splitcmd)
  if subprocess.call(splitcmd) != 0:
    raise RuntimeError("Unable to kill TMaster")
  logging.info("Killed tmaster")

def getProcesses():
  '''
  returns a list of process tuples (pid, cmd)
  This only applies only for local scheduler as it uses the ps command
  and assumes the topology will be running on different processes on same machine
  '''
  processes = subprocess.check_output(['ps', '-o', 'pid,args'])
  processes = processes.split('\n')
  processes = processes[1:] # remove first line, which is name of columns
  processList = []
  for process in processes:
    # remove empty lines
    if process == '':
      continue
    pretuple = process.split(' ', 1)
    processList.append(ProcessTuple(pretuple[0], pretuple[1]))
  return processList

def getPid(processName, heronWorkingDirectory):
  '''
  opens .pid file of process and reads the first and only line, which should be the process pid
  if fail, return -1
  '''
  processPidFile = os.path.join(heronWorkingDirectory, processName + '.pid')
  try:
    with open(processPidFile, 'r') as f:
      pid = f.readline()
      return pid
  except Exception:
    print("Unable to open file %s", processPidFile)
    logging.error("Unable to open file %s", processPidFile)
    return -1

def killProcess(processNumber):
  ''' kills process by running unix command kill '''
  logging.info("Killing process number %s", processNumber)

  try:
    os.kill(int(processNumber), signal.SIGTERM)
  except OSError as ex:
    if "No such process" in str(ex): # killing a non-existing process condsidered as success
      logging.info(str(ex))
    else:
      raise RuntimeError("Unable to kill process %s" % processNumber)
  except Exception:
    raise RuntimeError("Unable to kill process %s" % processNumber)

  logging.info("Killed process number %s", processNumber)

def processExists(processList, processCmd):
  ''' check if a process is running '''
  for process in processList:
    if processCmd in process.cmd:
      return True
  return False

def main():
  ''' main '''
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)

  # Read the configuration file from package
  conf_file = DEFAULT_TEST_CONF_FILE
  confString = pkgutil.get_data(__name__, conf_file)
  decoder = json.JSONDecoder(strict=False)

  # Convert the conf file to a json format
  conf = decoder.decode(confString)

  # Get the directory of the heron root, which should be the directory that the script is run from
  heronRepoDirectory = os.getcwd()

  args = dict()
  homeDirectory = os.path.expanduser("~")
  args['cluster'] = conf['cluster']
  args['topologyName'] = conf['topology']['topologyName']
  args['topologyClassPath'] = conf['topology']['topologyClassPath']
  args['workingDirectory'] = os.path.join(
      homeDirectory,
      ".herondata",
      "topologies",
      conf['cluster'],
      getpass.getuser(),
      args['topologyName']
  )
  args['cliPath'] = os.path.expanduser(conf['heronCliPath'])
  args['outputFile'] = os.path.join(args['workingDirectory'], conf['topology']['outputFile'])
  args['readFile'] = os.path.join(args['workingDirectory'], conf['topology']['readFile'])
  args['testJarPath'] = os.path.join(heronRepoDirectory, conf['testJarPath'])

  start_time = time.time()
  (successes, failures) = runAllTests(args)
  elapsed_time = time.time() - start_time

  if not failures:
    logging.info("Success: %s (all) tests passed", len(successes))
    logging.info("Elapsed time: %s", elapsed_time)
    sys.exit(0)
  else:
    logging.error("Fail: %s test failed", len(failures))
    logging.info("Failed Tests: ")
    logging.info("\n".join(failures))
    sys.exit(1)

if __name__ == '__main__':
  main()
