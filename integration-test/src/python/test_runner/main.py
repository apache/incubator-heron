''' main '''
import argparse
import json
import logging
import os
import pkgutil
import sys
import time
import uuid
# pylint: disable=unused-wildcard-import
from httplib import *

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "integration-test/src/python/test_runner/resources/test.conf"

RETRY_ATTEMPTS = 25
#seconds
RETRY_INTERVAL = 10

def runTest(topologyName, classPath, expectedResultFilePath, params):
  ''' Runs the test for one topology '''
  httpServerUrl = "http://" + params.resultsServerHostname + ":" + \
    params.resultsServerPort + "/results"

  #submit topology
  try:
    args = httpServerUrl + " " + topologyName
    submitTopology(params.heronCliPath, params.cluster, params.role,
                   params.env, params.testsBinPath, classPath,
                   params.releasePackageUri, args)
  except Exception as e:
    logging.error("Failed to submit %s topology: %s", topologyName, str(e))
    return "fail"

  logging.info("Successfully submitted %s topology", topologyName)

  # Fetch actual results. Sleep and retry if the result is not ready
  try:
    _, actualResult = fetchResultFromServer(
        params.resultsServerHostname,
        params.resultsServerPort,
        topologyName
    )
  except Exception as e:
    logging.error("Fetching result failed for %s topology: %s", topologyName, str(e))
    return "fail"
  finally:
    killTopology(params.heronCliPath, params.cluster, params.role, params.env, topologyName)

  # Read expected result from the expected result file
  try:
    if not os.path.exists(expectedResultFilePath):
      raise RuntimeError(" %s does not exist" % expectedResultFilePath)
    else:
      with open(expectedResultFilePath, "r") as expectedResultFile:
        expectedResult = expectedResultFile.read().rstrip()
  except Exception as e:
    logging.error("Failed to read expected result file %s: %s", expectedResultFilePath, str(e))
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
    logging.info("Topology %s result matches expected result", topologyName)
    return "success"
  else:
    logging.error("Actual result did not match expected result")
    logging.info("Actual result ---------- \n" + str(actualResult))
    logging.info("Expected result ---------- \n" + str(expectedResult))
    return "fail"

def fetchResultFromServer(serverAddress, serverPort, topologyName):
  ''' Make a http get request to fetch actual results from http server '''
  for i in range(0, RETRY_ATTEMPTS):
    logging.info("Fetching results for topology %s, retry count: %d", topologyName, i)
    response = getHTTPResponse(serverAddress, int(serverPort), topologyName)
    if response.status == 200:
      return (response.status, response.read())
    elif i != RETRY_ATTEMPTS:
      logging.info("Fetching results failed with status: %s; reason: %s; body: %s",
                   response.status, response.reason, response.read())
      time.sleep(RETRY_INTERVAL)

  logging.error("Failed to fetch results after %d attempts", RETRY_ATTEMPTS)
  raise RuntimeError("exceeded fetching result retry attempts")

def getHTTPResponse(serverAddress, serverPort, topologyName):
  ''' get HTTP response '''
  # pylint: disable=unused-variable
  for i in range(0, RETRY_ATTEMPTS):
    try:
      connection = HTTPConnection(serverAddress, int(serverPort))
      connection.request('GET', '/results/' + topologyName)
      response = connection.getresponse()
      return response
    except Exception:
      time.sleep(RETRY_INTERVAL)
      continue

  logging.error("Failed to get HTTP Response after %d attempts", RETRY_ATTEMPTS)
  raise RuntimeError("Failed to get HTTP response")

def submitTopology(heronCliPath, cluster, role, env, jarPath, classPath, pkgUri, args=None):
  ''' Submit topology using heron-cli '''
  logging.info("Submitting topology")

  # Form the command to submit a topology.
  # Note the single quote around the arg for heron.package.core.uri.
  # This is needed to prevent shell expansion.
  cmd = "%s submit %s/%s/%s %s %s %s --verbose" % (
      heronCliPath, cluster, role, env, jarPath, classPath, args)

  if pkgUri is not None:
    cmd = "%s --config-property heron.package.core.uri='%s'" %(cmd, pkgUri)

  logging.info("Submitting command: %s", cmd)

  for _ in range(0, RETRY_ATTEMPTS):
    if os.system(cmd) == 0:
      logging.info("Successfully submitted topology")
      return

  raise RuntimeError("Unable to submit the topology")

def killTopology(heronCliPath, cluster, role, env, topologyName):
  ''' Kill a topology using heron-cli '''
  logging.info("Killing topology")
  cmd = "%s kill %s/%s/%s %s --verbose" % (
      heronCliPath, cluster, role, env, topologyName)

  logging.info("Submitting command: %s", cmd)
  for i in range(0, RETRY_ATTEMPTS):
    if os.system(cmd) != 0:
      time.sleep(RETRY_INTERVAL)
      logging.warning("killing topology %s with %d attempts", topologyName, i)
    else:
      logging.info("Successfully killed topology %s", topologyName)
      return

  logging.error("Failed to kill topology %s", topologyName)
  raise RuntimeError("Unable to kill the topology")

def runAllTests(conf, args):
  ''' Run the test for each topology specified in the conf file '''
  successes = []
  failures = []
  timestamp = str(int(time.time()))

  if args.testsBinPath.endswith(".jar"):
    test_topologies = conf["javaTopologies"]
    pkg_type = "jar"
  elif args.testsBinPath.endswith(".pex"):
    test_topologies = conf["pythonTopologies"]
    pkg_type = "pex"
  else:
    raise ValueError("Unrecognized binary file type: %s" % args.testsBinPath)

  total = len(test_topologies)
  current = 1

  for topologyConf in test_topologies:
    topologyName = ("%s_%s_%s") % (timestamp, topologyConf["topologyName"], str(uuid.uuid4()))
    if pkg_type == 'pex':
      classPath = topologyConf["classPath"]
    elif pkg_type == 'jar':
      classPath = conf["topologyClasspathPrefix"] + topologyConf["classPath"]
    else:
      raise ValueError("Unrecognized package type: %s" % pkg_type)

    expectedResultFilePath = args.topologiesPath + "/" + topologyConf["expectedResultRelativePath"]

    logging.info("==== Starting test %s of %s: %s ====", current, total, topologyName)
    start_secs = int(time.time())
    if runTest(topologyName, classPath, expectedResultFilePath, args) == "success":
      successes += [(topologyName, int(time.time()) - start_secs)]
    else:
      failures += [(topologyName, int(time.time()) - start_secs)]
    current += 1
  return (successes, failures)

def main():
  ''' main '''
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)
  print(os.path.join(os.path.dirname(sys.modules[__name__].__file__), DEFAULT_TEST_CONF_FILE), 'rb')
  conf_file = DEFAULT_TEST_CONF_FILE
  # Read the configuration file from package
  confString = pkgutil.get_data(__name__, conf_file)
  decoder = json.JSONDecoder(strict=False)
  # Convert the conf file to a json format
  conf = decoder.decode(confString)

  # Parse the arguments passed via command line
  parser = argparse.ArgumentParser(description='This is the heron integration test framework')

  parser.add_argument('-hc', '--heron-cli-path', dest='heronCliPath', default=conf['heronCliPath'])
  parser.add_argument('-tb', '--tests-bin-path', dest='testsBinPath')
  parser.add_argument('-cl', '--cluster', dest='cluster', default=conf['cluster'])
  parser.add_argument('-ev', '--env', dest='env', default=conf['env'])
  parser.add_argument('-rl', '--role', dest='role', default=conf['role'])
  parser.add_argument('-rh', '--results-server-hostname', dest='resultsServerHostname')
  parser.add_argument('-rp', '--results-server-port', dest='resultsServerPort',
                      default=conf['resultsServerPort'])
  parser.add_argument('-tp', '--topologies-path', dest='topologiesPath')
  parser.add_argument('-pi', '--release-package-uri', dest='releasePackageUri', default=None)

  #parser.add_argument('-dt', '--disable-topologies', dest='disabledTopologies', default='',
  #                    help='comma separated test case(classpath) name that will not be run')
  #parser.add_argument('-et', '--enable-topologies', dest='enableTopologies', default=None,
  #                    help='comma separated test case(classpath) name that will be run only')

  args = parser.parse_args()

  (successes, failures) = runAllTests(conf, args)
  total = len(failures) + len(successes)

  if not failures:
    logging.info("SUCCESS: %s (all) tests passed:", len(successes))
    for test in successes:
      logging.info("  - %s: %s", ("[%ss]" % test[1]).ljust(8), test[0])
    sys.exit(0)
  else:
    logging.error("FAILURE: %s/%s tests failed:", len(failures), total)
    for test in failures:
      logging.error("  - %s: %s", ("[%ss]" % test[1]).ljust(8), test[0])
    sys.exit(1)

if __name__ == '__main__':
  main()
