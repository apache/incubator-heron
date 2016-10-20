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

def run_test(topology_name, classpath, expected_result_file_path,
             params, http_server_host_port, update_args, extra_topology_args):
  ''' Runs the test for one topology '''

  #submit topology
  try:
    args = "-r http://%s/results -t %s %s" %\
           (http_server_host_port, topology_name, extra_topology_args)
    submit_topology(params.heron_cli_path, params.cli_config_path, params.cluster, params.role,
                    params.env, params.tests_bin_path, classpath,
                    params.release_package_uri, args)
  except Exception as e:
    logging.error("Failed to submit %s topology: %s", topology_name, str(e))
    return "fail"

  logging.info("Successfully submitted %s topology", topology_name)

  try:
    if update_args:
      # wait for the topology to be started before triggering an update
      poll_state_server(http_server_host_port, topology_name, "topology_started")
      logging.info("Verified topology successfully started, proceeding to update it")
      update_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
                      params.role, params.env, topology_name, update_args)

      # update state server to trigger more emits from the topology
      logging.info("Topology successfully updated, updating state server")
      update_state_server(http_server_host_port, topology_name, "topology_updated", "true")

    return check_results(http_server_host_port, topology_name, expected_result_file_path)

  except Exception as e:
    logging.error("Checking result failed for %s topology: %s", topology_name, str(e))
    return "fail"
  finally:
    kill_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
                  params.role, params.env, topology_name)

# pylint: disable=unnecessary-lambda
def check_results(server_host_port, topology_name, expected_result_file_path):
  """ Checks the topology results from the server with the expected results from the file """
  # Fetch actual results. Sleep and retry if the result is not ready
  try:
    actual_result = fetch_result_from_server(server_host_port, topology_name)
  except Exception as e:
    logging.error("Fetching result failed for %s topology: %s", topology_name, str(e))
    return "fail"

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
    # lambda required below to remove the unicode 'u' from the output
    logging.info("Actual result ---------- \n" + str(map(lambda x: str(x), actual_result)))
    logging.info("Expected result ---------- \n" + str(map(lambda x: str(x), expected_result)))
    return "fail"

def fetch_result_from_server(server_host_port, topology_name):
  return fetch_from_server(
      server_host_port, topology_name, 'results', '/results/%s' % topology_name)

def poll_state_server(server_host_port, topology_name, key):
  return fetch_from_server(
      server_host_port, topology_name, key, '/state/%s_%s' % (topology_name, key))

def update_state_server(http_server_host_port, topology_name, key, value):
  connection = HTTPConnection(http_server_host_port)
  connection.request('POST', '/state/%s_%s' % (topology_name, key), '"%s"' % value)
  response = connection.getresponse()
  return response.status == 200

def fetch_from_server(server_host_port, topology_name, data_name, path):
  ''' Make a http get request to fetch actual results from http server '''
  for i in range(0, RETRY_ATTEMPTS):
    logging.info("Fetching %s for topology %s, retry count: %d", data_name, topology_name, i)
    response = get_http_response(server_host_port, path)
    if response.status == 200:
      return response.read()
    elif i != RETRY_ATTEMPTS:
      logging.info("Fetching %s failed with status: %s; reason: %s; body: %s",
                   data_name, response.status, response.reason, response.read())
      time.sleep(RETRY_INTERVAL)

  logging.error("Failed to fetch %s after %d attempts", data_name, RETRY_ATTEMPTS)
  raise RuntimeError("exceeded fetching %s retry attempts", data_name)

def get_http_response(server_host_port, path):
  ''' get HTTP response '''
  # pylint: disable=unused-variable
  for i in range(0, RETRY_ATTEMPTS):
    try:
      connection = HTTPConnection(server_host_port)
      connection.request('GET', path)
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
  # Form the command to submit a topology.
  # Note the single quote around the arg for heron.package.core.uri.
  # This is needed to prevent shell expansion.
  cmd = "%s submit --config-path=%s %s %s %s %s" %\
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env),
         jar_path, classpath, args)

  if pkg_uri is not None:
    cmd = "%s --config-property heron.package.core.uri='%s'" %(cmd, pkg_uri)

  logging.info("Submitting topology: %s", cmd)

  if os.system(cmd) == 0:
    return

  raise RuntimeError("Unable to submit the topology")

def kill_topology(heron_cli_path, cli_config_path, cluster, role, env, topology_name):
  ''' Kill a topology using heron-cli '''
  cmd = "%s kill --config-path=%s %s %s" %\
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env), topology_name)

  logging.info("Kill topology: %s", cmd)
  if os.system(cmd) == 0:
    logging.info("Successfully killed topology %s", topology_name)
    return

  logging.error("Failed to kill topology %s", topology_name)
  raise RuntimeError("Unable to kill the topology")

def update_topology(heron_cli_path, cli_config_path, cluster,
                    role, env, topology_name, update_args):
  cmd = "%s update --config-path=%s %s %s %s --verbose" %\
        (heron_cli_path, cli_config_path,
         cluster_token(cluster, role, env), update_args, topology_name)

  logging.info("Update topology: %s", cmd)
  if os.system(cmd) == 0:
    logging.info("Successfully updated topology %s", topology_name)
    return

  raise RuntimeError("Failed to update topology %s", topology_name)

def run_tests(conf, args):
  ''' Run the test for each topology specified in the conf file '''
  successes = []
  failures = []
  timestamp = time.strftime('%Y%m%d%H%M%S')

  http_server_host_port = "%s:%d" % (args.http_server_hostname, args.http_server_port)

  if args.tests_bin_path.endswith(".jar"):
    test_topologies = conf["javaTopologies"]
    topology_classpath_prefix = conf["topologyClasspathPrefix"]
    extra_topology_args = "-s http://%s/state" % http_server_host_port
  elif args.tests_bin_path.endswith(".pex"):
    test_topologies = conf["pythonTopologies"]
    topology_classpath_prefix = ""
    extra_topology_args = ""
  else:
    raise ValueError("Unrecognized binary file type: %s" % args.tests_bin_path)

  initial_topologies = test_topologies
  if args.test_topology_pattern:
    pattern = re.compile(args.test_topology_pattern)
    test_topologies = filter(lambda x: pattern.match(x['topologyName']), test_topologies)

  total = len(test_topologies)
  current = 1

  if total == 0:
    logging.error("Test filter '%s' did not match any configured test names:\n%s",
                  args.test_topology_pattern,
                  '\n'.join(map(lambda x: x['topologyName'], initial_topologies)))
    sys.exit(1)

  for topology_conf in test_topologies:
    topology_name = ("%s_%s_%s") % (timestamp, topology_conf["topologyName"], str(uuid.uuid4()))
    classpath = topology_classpath_prefix + topology_conf["classPath"]

    # if the test includes an update we need to pass that info to the topology so it can send
    # data accordingly. This flag causes the test spout to emit, then check the state of this
    # token, then emit more.
    update_args = ""
    topology_args = extra_topology_args
    if "updateArgs" in topology_conf:
      topology_args = "%s -u topology_updated" % extra_topology_args
      update_args = topology_conf["updateArgs"]

    expected_result_file_path =\
      args.topologies_path + "/" + topology_conf["expectedResultRelativePath"]

    logging.info("==== Starting test %s of %s: %s ====", current, total, topology_name)
    start_secs = int(time.time())
    if run_test(topology_name, classpath, expected_result_file_path,
                args, http_server_host_port, update_args, topology_args) == "success":
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
  parser.add_argument('-rh', '--http-server-hostname', dest='http_server_hostname')
  parser.add_argument('-rp', '--http-server-port', dest='http_server_port', type=int,
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
