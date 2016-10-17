''' main '''
import argparse
import json
import logging
import os
import pkgutil
import re
import sys
import time
import uuid
# pylint: disable=unused-wildcard-import
from httplib import *

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "integration-test/src/python/test_runner/resources/test.json"

RETRY_ATTEMPTS = 15
#seconds
RETRY_INTERVAL = 10

def run_test(topology_name, classpath, expected_result_file_path, params):
  ''' Runs the test for one topology '''
  http_results_server_url = "http://%s:%d/results" %\
                    (params.results_server_hostname, params.results_server_port)

  #submit topology
  try:
    args = "-r %s -t %s" % (http_results_server_url, topology_name)
    submit_topology(params.heron_cli_path, params.cli_config_path, params.cluster, params.role,
                    params.env, params.tests_bin_path, classpath,
                    params.release_package_uri, args)
  except Exception as e:
    logging.error("Failed to submit %s topology: %s", topology_name, str(e))
    return "fail"

  logging.info("Successfully submitted %s topology", topology_name)

  # Fetch actual results. Sleep and retry if the result is not ready
  try:
    _, actual_result = fetch_result_from_server(
        params.results_server_hostname,
        params.results_server_port,
        topology_name
    )
  except Exception as e:
    logging.error("Fetching result failed for %s topology: %s", topology_name, str(e))
    return "fail"
  finally:
    kill_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
                  params.role, params.env, topology_name)

  # Read expected result from the expected result file
  try:
    if not os.path.exists(expected_result_file_path):
      raise RuntimeError(" %s does not exist" % expected_result_file_path)
    else:
      with open(expected_result_file_path, "r") as expected_result_file:
        expected_result = expected_result_file.read().rstrip()
  except Exception as e:
    logging.error("Failed to read expected result file %s: %s", expected_result_file_path, str(e))
    return "fail"

  # Build a new instance of json decoder since the default one could not ignore "\n"
  decoder = json.JSONDecoder(strict=False)

  # The Heron doesn't guarantee the order of messages in any case, so we should sort the result.
  # Notice: here we treat every data unique even they are the same,
  # since we could judge here whether two messages are duplicates or not.
  # User may deal with emit message along with MESSAGE_ID or remove duplicates in topology.
  actual_result = sorted(decoder.decode(actual_result))
  expected_result = sorted(decoder.decode(expected_result))

  # Compare the actual and expected result
  if actual_result == expected_result:
    logging.info("Topology %s result matches expected result", topology_name)
    return "success"
  else:
    logging.error("Actual result did not match expected result")
    logging.info("Actual result ---------- \n" + str(actual_result))
    logging.info("Expected result ---------- \n" + str(expected_result))
    return "fail"

def fetch_result_from_server(server_address, server_port, topology_name):
  ''' Make a http get request to fetch actual results from http server '''
  for i in range(0, RETRY_ATTEMPTS):
    logging.info("Fetching results for topology %s, retry count: %d", topology_name, i)
    response = get_http_response(server_address, server_port, topology_name)
    if response.status == 200:
      return (response.status, response.read())
    elif i != RETRY_ATTEMPTS:
      logging.info("Fetching results failed with status: %s; reason: %s; body: %s",
                   response.status, response.reason, response.read())
      time.sleep(RETRY_INTERVAL)

  logging.error("Failed to fetch results after %d attempts", RETRY_ATTEMPTS)
  raise RuntimeError("exceeded fetching result retry attempts")

def get_http_response(server_address, server_port, topology_name):
  ''' get HTTP response '''
  # pylint: disable=unused-variable
  for i in range(0, RETRY_ATTEMPTS):
    try:
      connection = HTTPConnection(server_address, server_port)
      connection.request('GET', '/results/' + topology_name)
      response = connection.getresponse()
      return response
    except Exception:
      time.sleep(RETRY_INTERVAL)
      continue

  logging.error("Failed to get HTTP Response after %d attempts", RETRY_ATTEMPTS)
  raise RuntimeError("Failed to get HTTP response")

def cluster_token(cluster, role, env):
  if cluster == "local":
    return cluster
  return "%s/%s/%s" % (cluster, role, env)

def submit_topology(heron_cli_path, cli_config_path, cluster, role,
                    env, jar_path, classpath, pkg_uri, args=None):
  ''' Submit topology using heron-cli '''
  logging.info("Submitting topology")

  # Form the command to submit a topology.
  # Note the single quote around the arg for heron.package.core.uri.
  # This is needed to prevent shell expansion.
  cmd = "%s submit --config-path=%s %s %s %s %s" %\
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env),
         jar_path, classpath, args)

  if pkg_uri is not None:
    cmd = "%s --config-property heron.package.core.uri='%s'" %(cmd, pkg_uri)

  logging.info("Submitting command: %s", cmd)

  if os.system(cmd) == 0:
    logging.info("Successfully submitted topology")
    return

  raise RuntimeError("Unable to submit the topology")

def kill_topology(heron_cli_path, cli_config_path, cluster, role, env, topology_name):
  ''' Kill a topology using heron-cli '''
  logging.info("Killing topology")
  cmd = "%s kill --config-path=%s %s %s --verbose" %\
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env), topology_name)

  logging.info("Kill topology command: %s", cmd)
  if os.system(cmd) == 0:
    logging.info("Successfully killed topology %s", topology_name)
    return

  logging.error("Failed to kill topology %s", topology_name)
  raise RuntimeError("Unable to kill the topology")

def run_tests(conf, args):
  ''' Run the test for each topology specified in the conf file '''
  successes = []
  failures = []
  timestamp = time.strftime('%Y%m%d%H%M%S')

  if args.tests_bin_path.endswith(".jar"):
    test_topologies = conf["javaTopologies"]
    pkg_type = "jar"
  elif args.tests_bin_path.endswith(".pex"):
    test_topologies = conf["pythonTopologies"]
    pkg_type = "pex"
  else:
    raise ValueError("Unrecognized binary file type: %s" % args.tests_bin_path)

  if args.test_topology_pattern:
    pattern = re.compile(args.test_topology_pattern)
    test_topologies = filter(lambda x: pattern.match(x['topologyName']), test_topologies)

  total = len(test_topologies)
  current = 1

  for topology_conf in test_topologies:
    topology_name = ("%s_%s_%s") % (timestamp, topology_conf["topologyName"], str(uuid.uuid4()))
    if pkg_type == 'pex':
      classpath = topology_conf["classPath"]
    elif pkg_type == 'jar':
      classpath = conf["topologyClasspathPrefix"] + topology_conf["classPath"]
    else:
      raise ValueError("Unrecognized package type: %s" % pkg_type)

    expected_result_file_path =\
      args.topologies_path + "/" + topology_conf["expectedResultRelativePath"]

    logging.info("==== Starting test %s of %s: %s ====", current, total, topology_name)
    start_secs = int(time.time())
    if run_test(topology_name, classpath, expected_result_file_path, args) == "success":
      successes += [(topology_name, int(time.time()) - start_secs)]
    else:
      failures += [(topology_name, int(time.time()) - start_secs)]
    current += 1
  return (successes, failures)

def main():
  ''' main '''
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)
  conf_file = DEFAULT_TEST_CONF_FILE
  # Read the configuration file from package
  conf_string = pkgutil.get_data(__name__, conf_file)
  decoder = json.JSONDecoder(strict=False)
  # Convert the conf file to a json format
  conf = decoder.decode(conf_string)

  # Parse the arguments passed via command line
  parser = argparse.ArgumentParser(description='This is the heron integration test framework')

  parser.add_argument('-hc', '--heron-cli-path', dest='heron_cli_path',
                      default=conf['heronCliPath'])
  parser.add_argument('-tb', '--tests-bin-path', dest='tests_bin_path')
  parser.add_argument('-cl', '--cluster', dest='cluster', default=conf['cluster'])
  parser.add_argument('-ev', '--env', dest='env', default=conf['env'])
  parser.add_argument('-rl', '--role', dest='role', default=conf['role'])
  parser.add_argument('-rh', '--results-server-hostname', dest='results_server_hostname')
  parser.add_argument('-rp', '--results-server-port', dest='results_server_port', type=int,
                      default=conf['resultsServerPort'])
  parser.add_argument('-tp', '--topologies-path', dest='topologies_path')
  parser.add_argument('-ts', '--test-topology-pattern', dest='test_topology_pattern', default=None)
  parser.add_argument('-pi', '--release-package-uri', dest='release_package_uri', default=None)
  parser.add_argument('-cd', '--cli-config-path', dest='cli_config_path',
                      default=conf['cliConfigPath'])

  #parser.add_argument('-dt', '--disable-topologies', dest='disabledTopologies', default='',
  #                    help='comma separated test case(classpath) name that will not be run')
  #parser.add_argument('-et', '--enable-topologies', dest='enableTopologies', default=None,
  #                    help='comma separated test case(classpath) name that will be run only')

  args, unknown_args = parser.parse_known_args()
  if unknown_args:
    logging.error('Unknown argument passed to %s: %s', sys.argv[0], unknown_args[0])
    sys.exit(1)

  (successes, failures) = run_tests(conf, args)
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
