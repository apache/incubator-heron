import argparse
import json
import logging
import os
import pkgutil
import sys
import threading
import time
import uuid
from httplib import *

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "integration-test/src/python/test_runner/resources/test.conf"

RETRY_ATTEMPTS = 15
#seconds
RETRY_INTERVAL = 30

#Runs the test for one topology
def runTest(topologyName, classPath, expectedResultFilePath, params):
  httpServerUrl = "http://" + params.resultsServerHostname + ":" + params.resultsServerPort + "/results"

  #submit topology
  try:
    args = httpServerUrl + " " + topologyName
    submitTopology(params.heronCliPath, params.dataCenter, params.role, params.env, params.testsJarPath, classPath,
      params.releasePackageRole, params.releasePackageName, args)
  except Exception as e:
    logging.error("Failed to submit %s topology: %s" %(topologyName, str(e)))
    return "fail"

  logging.info("Successfully submitted %s topology" %(topologyName))

  # Fetch actual results. Sleep and retry if the result is not ready
  try:
    (httpResponseCode, actualResult) = fetchResultFromServer(
      params.resultsServerHostname,
      params.resultsServerPort,
      topologyName
    )
  except Exception as e:
    logging.error("Fetching result failed for %s topology: %s" %(topologyName, str(e)))
    return "fail"
  finally:
    killTopology(params.heronCliPath, params.dataCenter, params.role, params.env, topologyName)

  # Read expected result from the expected result file
  try:
    if not os.path.exists(expectedResultFilePath):
      raise RuntimeError(" %s does not exist" % expectedResultFilePath)
    else:
      with open(expectedResultFilePath, "r") as expectedResultFile:
        expectedResult = expectedResultFile.read().rstrip()
  except Exception as e:
    logging.error("Failed to read expected result file %s: %s" %(expectedResultFilePath, str(e)))
    return "fail"

  # Build a new instance of json decoder since the default one could not ignore "\n"
  decoder = json.JSONDecoder(strict=False)

  # The Heron doesn't guarantee the order of messages in any case, so we should sort the result.
  # Notice: here we treat every data unique even they are the same,
  # since we could judge here whether two messages are duplicates or not.
  # User may deal with emit message along with MESSAGE_ID or remove duplicates in topology.
  actualResult = sorted(decoder.decode(actualResult))
  expectedResult = sorted(decoder.decode(expectedResult))

  # Compare the actual and expected result
  if actualResult == expectedResult:
    logging.info("Topology %s result matches expected result" % topologyName)
    return "success"
  else:
    logging.error("Actual result did not match expected result")
    logging.info("Actual result ---------- \n" + actualResult)
    logging.info("Expected result ---------- \n" + expectedResult)
    return "fail"

# Make a http get request to fetch actual results from http server
def fetchResultFromServer(serverAddress, serverPort, topologyName):
  for i in range(0, RETRY_ATTEMPTS):
    logging.info("Fetching results for topology %s, retry count: %d" % (topologyName, i))
    response = getHTTPResponse(serverAddress, int(serverPort), topologyName)
    if response.status == 200:
      return (response.status, response.read())
    elif i != RETRY_ATTEMPTS:
      logging.info("Fetching results failed with status: %s; reason: %s; body: %s"
        % (response.status, response.reason, response.read()))
      time.sleep(RETRY_INTERVAL)

  logging.error("Failed to fetch results after %d attempts" % RETRY_ATTEMPTS)
  raise RuntimeError("exceeded fetching result retry attempts")

def getHTTPResponse(serverAddress, serverPort, topologyName):
  for i in range (0, RETRY_ATTEMPTS):
    try:
      connection = HTTPConnection(serverAddress, int(serverPort))
      connection.request('GET', '/results/' + topologyName)
      response = connection.getresponse()
      return response
    except Exception:
      time.sleep(RETRY_INTERVAL)
      continue

  logging.error("Failed to get HTTP Response after %d attempts" % RETRY_ATTEMPTS)
  raise RuntimeError("Failed to get HTTP response")

# Submit topology using heron-cli
def submitTopology(heronCliPath, dc, role, env, jarPath, classPath, pkgRole, pkgName, args = None):
  logging.info("Submitting topology")
  cmd = ("%s submit %s/%s/%s --heron-release-pkgrole=%s"
        " --heron-release-pkgname=%s %s %s %s --verbose" % (
    heronCliPath, dc, role, env, pkgRole, pkgName, jarPath, classPath, args))

  logging.info("Submitting command: %s" % (cmd))

  for i in range(0, RETRY_ATTEMPTS):
    if (os.system(cmd) == 0):
      logging.info("Successfully submitted topology")
      return

  raise RuntimeError("Unable to submit the topology")

# Kill a topology using heron-cli
def killTopology(heronCliPath, dc, role, env, topologyName):
  logging.info("Killing topology")
  cmd = "%s kill %s/%s/%s %s --verbose" % (
    heronCliPath, dc, role, env, topologyName)

  logging.info("Submitting command: %s" % (cmd))
  for i in range(0, RETRY_ATTEMPTS):
    if (os.system(cmd) != 0):
      time.sleep(RETRY_INTERVAL)
      logging.warning("killing topology %s with %d attempts" % (topologyName, i))
    else:
       logging.info("Successfully killed topology %s" % topologyName)
       return

  logging.error("Failed to kill topology %s" % topologyName)
  raise RuntimeError("Unable to kill the topology")

# Run the test for each topology specified in the conf file
def runAllTests(conf, args):
  successes = []
  failures = []
  timestamp = str(int(time.time()))
  for topologyConf in conf["topologies"]:
    topologyName = ("%s_%s_%s") % (timestamp, topologyConf["topologyName"], str(uuid.uuid4()))
    classPath = conf["topologyClasspathPrefix"] + topologyConf["classPath"]
    expectedResultFilePath = args.topologiesPath + "/" + topologyConf["expectedResultRelativePath"]

    if (runTest(topologyName, classPath, expectedResultFilePath, args) == "success"):
      successes += [topologyName]
    else:
      failures += [topologyName]
  return (successes, failures)

def kinit(interval):
  cmd = "kinit -k -t /etc/twkeys/jenkins/kerberos/jenkins.keytab jenkins@TWITTER.BIZ"
  while True:
    if os.system(cmd) == 0:
      logging.info("Successfully kinited")
    else:
      logging.info("Failed to kinit")

    time.sleep(interval)

def main():
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)
  print (os.path.join(os.path.dirname(sys.modules[__name__].__file__), DEFAULT_TEST_CONF_FILE), 'rb')
  conf_file = DEFAULT_TEST_CONF_FILE
  # Read the configuration file from package
  confString = pkgutil.get_data(__name__, conf_file)
  decoder = json.JSONDecoder(strict=False)
  # Convert the conf file to a json format
  conf = decoder.decode(confString)

  # schedule kinit
  kinit_thread = threading.Thread(target = kinit, name = "cron_kinit", args=(900,))
  kinit_thread.setDaemon(True)
  kinit_thread.start()

  # Parse the arguments passed via command line
  parser = argparse.ArgumentParser(description='This is the heron integration test framework')

  parser.add_argument('-hc', '--heron-cli-path', dest='heronCliPath', default=conf['heronCliPath'])
  parser.add_argument('-tj', '--tests-jar-path', dest='testsJarPath')
  parser.add_argument('-dc', '--data-center', dest='dataCenter', default=conf['dataCenter'])
  parser.add_argument('-ev', '--env', dest='env', default=conf['env'])
  parser.add_argument('-rl', '--role', dest='role', default=conf['role'])
  parser.add_argument('-rh', '--results-server-hostname', dest='resultsServerHostname')
  parser.add_argument('-rp', '--results-server-port', dest='resultsServerPort', default=conf['resultsServerPort'])
  parser.add_argument('-tp', '--topologies-path', dest='topologiesPath')
  parser.add_argument('-pn', '--release-package-name', dest='releasePackageName', default=conf['releasePackageName'])
  parser.add_argument('-pr', '--release-package-role', dest='releasePackageRole', default=conf['releasePackageRole'])

  #TODO: Enable this option
  #parser.add_argument('-dt', '--disable-topologies', dest='disabledTopologies', default='',
  #                    help='comma separated test case(classpath) name that will not be run')
  #parser.add_argument('-et', '--enable-topologies', dest='enableTopologies', default=None,
  #                    help='comma separated test case(classpath) name that will be run only')

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
