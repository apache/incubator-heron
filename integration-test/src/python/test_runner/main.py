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
from httplib import HTTPConnection

from ..common import status
from heron.common.src.python.utils import log

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "integration-test/src/python/test_runner/resources/test.json"

RETRY_ATTEMPTS = 15
#seconds
RETRY_INTERVAL = 10

class FileBasedExpectedResultsHandler(object):
  def __init__(self, file_path):
    self.file_path = file_path

  def fetch_results(self):
    # Read expected result from the expected result file
    try:
      if not os.path.exists(self.file_path):
        raise status.TestFailure("Expected results file %s does not exist" % self.file_path)
      else:
        with open(self.file_path, "r") as expected_result_file:
          return expected_result_file.read().rstrip()
    except Exception as e:
      raise status.TestFailure("Failed to read expected result file %s" % self.file_path, e)

class HttpBasedExpectedResultsHandler(object):
  def __init__(self, server_host_port, topology_name, task_count):
    self.server_host_port = server_host_port
    self.topology_name = topology_name
    self.task_count = task_count

  # pylint: disable=unnecessary-lambda
  def fetch_results(self):
    try:
      result = []
      decoder = json.JSONDecoder(strict=False)
      for i in range(0, self.task_count):
        task_result = fetch_from_server(self.server_host_port, self.topology_name,
                                        'expected results',
                                        '/state/%s_tuples_emitted_%d' % (self.topology_name, i))
        json_result = decoder.decode(task_result)
        logging.info("Found %d tuples emitted from spout task %d", len(json_result), i)
        result = result + json_result

      if len(result) == 0:
        raise status.TestFailure(
            "Expected result set is empty for topology %s" % self.topology_name)

      # need to convert from a list of json objects to a string of a python list,
      # without the unicode using double quotes, not single quotes.
      return str(map(lambda x: str(x), result)).replace("'", '"')
    except Exception as e:
      raise status.TestFailure(
          "Fetching expected result failed for %s topology" % self.topology_name, e)

class HttpBasedActualResultsHandler(object):
  def __init__(self, server_host_port, topology_name):
    self.server_host_port = server_host_port
    self.topology_name = topology_name

  def fetch_results(self):
    try:
      return fetch_from_server(self.server_host_port, self.topology_name,
                               'results', '/results/%s' % self.topology_name)
    except Exception as e:
      raise status.TestFailure("Fetching result failed for %s topology" % self.topology_name, e)

# pylint: disable=unnecessary-lambda
class ExactlyOnceResultsChecker(object):
  """Compares what results we found against what was expected. Verifies and exact match"""

  def __init__(self, topology_name, expected_results_handler, actual_results_handler):
    self.topology_name = topology_name
    self.expected_results_handler = expected_results_handler
    self.actual_results_handler = actual_results_handler

  def check_results(self):
    """ Checks the topology results from the server with the expected results from the file """
    actual_result = self.actual_results_handler.fetch_results()
    expected_result = self.expected_results_handler.fetch_results()

    # Build a new instance of json decoder since the default one could not ignore "\n"
    decoder = json.JSONDecoder(strict=False)

    # The Heron doesn't guarantee the order of messages in any case, so we should sort the result.
    # Notice: here we treat every data unique even they are the same,
    # since we could judge here whether two messages are duplicates or not.
    # User may deal with emit message along with MESSAGE_ID or remove duplicates in topology.
    actual_results = sorted(decoder.decode(actual_result))
    expected_results = sorted(decoder.decode(expected_result))
    return self._compare(expected_results, actual_results)

  def _compare(self, expected_results, actual_results):
    # Compare the actual and expected result
    if actual_results == expected_results:
      return status.TestSuccess(
          "Topology %s result matches expected result: %s expected tuples found exactly once" %
          (len(expected_results), self.topology_name))
    else:
      failure = status.TestFailure("Actual result did not match expected result")
      # lambda required below to remove the unicode 'u' from the output
      logging.info("Actual result ---------- \n" + str(map(lambda x: str(x), actual_results)))
      logging.info("Expected result ---------- \n" + str(map(lambda x: str(x), expected_results)))
      raise failure

class AtLeastOnceResultsChecker(ExactlyOnceResultsChecker):
  """Compares what results we found against what was expected. Verifies and exact match"""

  def _compare(self, expected_results, actual_results):
    expected_counts = _frequency_dict(expected_results)
    actual_counts = _frequency_dict(actual_results)
    missed_counts = {}
    for expected_value in expected_counts:
      expected_count = expected_counts[expected_value]
      if expected_value in actual_counts:
        actual_count = actual_counts[expected_value]
        if actual_count < expected_count:
          missed_counts[expected_value] = expected_count
      else:
        missed_counts[expected_value] = expected_count

    if len(missed_counts) == 0:
      return status.TestSuccess(
          "Topology %s result matches expected result: %s expected tuples found at least once" %
          (self.topology_name, len(expected_counts)))
    else:
      failure = status.TestFailure("Actual result did not match expected result")
      # lambda required below to remove the unicode 'u' from the output
      logging.info("Actual value frequencies ---------- \n" + ', '.join(
          map(lambda (k, v): "%s(%s)" % (str(k), v), actual_counts.iteritems())))
      logging.info("Expected value frequencies ---------- \n" + ', '.join(
          map(lambda (k, v): "%s(%s)" % (str(k), v), expected_counts.iteritems())))
      raise failure

def _frequency_dict(values):
  frequency = {}
  for value in values:
    count = 0
    if value in frequency:
      count = frequency[value]
    frequency[value] = count + 1
  return frequency

def run_test(topology_name, classpath, results_checker,
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
    raise status.TestFailure("Failed to submit %s topology" % topology_name, e)

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

    return results_checker.check_results()

  except Exception as e:
    raise status.TestFailure("Checking result failed for %s topology" % topology_name, e)
  finally:
    kill_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
                  params.role, params.env, topology_name)

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

  raise status.TestFailure("Failed to fetch %s after %d attempts" % (data_name, RETRY_ATTEMPTS))

def get_http_response(server_host_port, path):
  ''' get HTTP response '''
  for _ in range(0, RETRY_ATTEMPTS):
    try:
      connection = HTTPConnection(server_host_port)
      connection.request('GET', path)
      response = connection.getresponse()
      return response
    except Exception:
      time.sleep(RETRY_INTERVAL)
      continue

  raise status.TestFailure("Failed to get HTTP Response after %d attempts" % RETRY_ATTEMPTS)

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

  if os.system(cmd) != 0:
    raise status.TestFailure("Unable to submit the topology")

def kill_topology(heron_cli_path, cli_config_path, cluster, role, env, topology_name):
  ''' Kill a topology using heron-cli '''
  cmd = "%s kill --config-path=%s %s %s" %\
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env), topology_name)

  logging.info("Killing topology: %s", cmd)
  if os.system(cmd) != 0:
    raise status.TestFailure("Failed to kill topology %s" % topology_name)

  logging.info("Successfully killed topology %s", topology_name)

def update_topology(heron_cli_path, cli_config_path, cluster,
                    role, env, topology_name, update_args):
  cmd = "%s update --config-path=%s %s %s %s --verbose" %\
        (heron_cli_path, cli_config_path,
         cluster_token(cluster, role, env), update_args, topology_name)

  logging.info("Update topology: %s", cmd)
  if os.system(cmd) != 0:
    raise status.TestFailure("Failed to update topology %s" % topology_name)

  logging.info("Successfully updated topology %s", topology_name)

def filter_test_topologies(test_topologies, test_pattern):
  initial_topologies = test_topologies
  if test_pattern:
    pattern = re.compile(test_pattern)
    test_topologies = filter(lambda x: pattern.match(x['topologyName']), test_topologies)

  if len(test_topologies) == 0:
    logging.error("Test filter '%s' did not match any configured test names:\n%s",
                  test_pattern, '\n'.join(map(lambda x: x['topologyName'], initial_topologies)))
    sys.exit(1)
  return test_topologies

def run_tests(conf, args):
  ''' Run the test for each topology specified in the conf file '''
  successes = []
  failures = []
  timestamp = time.strftime('%Y%m%d%H%M%S')

  http_server_host_port = "%s:%d" % (args.http_server_hostname, args.http_server_port)

  if args.tests_bin_path.endswith(".jar"):
    test_topologies = filter_test_topologies(conf["javaTopologies"], args.test_topology_pattern)
    topology_classpath_prefix = conf["topologyClasspathPrefix"]
    extra_topology_args = "-s http://%s/state" % http_server_host_port
  elif args.tests_bin_path.endswith(".pex"):
    test_topologies = filter_test_topologies(conf["pythonTopologies"], args.test_topology_pattern)
    topology_classpath_prefix = ""
    extra_topology_args = ""
  else:
    raise ValueError("Unrecognized binary file type: %s" % args.tests_bin_path)

  current = 1
  for topology_conf in test_topologies:
    topology_name = ("%s_%s_%s") % (timestamp, topology_conf["topologyName"], str(uuid.uuid4()))
    classpath = topology_classpath_prefix + topology_conf["classPath"]

    # if the test includes an update we need to pass that info to the topology so it can send
    # data accordingly. This flag causes the test spout to emit, then check the state of this
    # token, then emit more.
    update_args = ""
    topology_args = extra_topology_args
    if "updateArgs" in topology_conf:
      update_args = topology_conf["updateArgs"]

    if "topologyArgs" in topology_conf:
      if topology_conf["topologyArgs"] == "emit_util" and update_args == "":
        raise ValueError("Specifying a test with emit_until spout wrapper without updateArgs "
                         + "will cause the spout to emit indefinitely. Not running topology "
                         + topology_name)
      topology_args = "%s %s" % (topology_args, topology_conf["topologyArgs"])

    results_checker = load_result_checker(
        topology_name, topology_conf,
        load_expected_result_handler(topology_name, topology_conf, args, http_server_host_port),
        HttpBasedActualResultsHandler(http_server_host_port, topology_name))

    logging.info("==== Starting test %s of %s: %s ====",
                 current, len(test_topologies), topology_name)
    start_secs = int(time.time())
    try:
      result = run_test(topology_name, classpath, results_checker,
                        args, http_server_host_port, update_args, topology_args)
      test_tuple = (topology_name, int(time.time()) - start_secs)
      if isinstance(result, status.TestSuccess):
        successes += [test_tuple]
      elif isinstance(result, status.TestFailure):
        failures += [test_tuple]
      else:
        logging.error("Unrecognized test response returned for test %s: %s",
                      topology_name, str(result))
        failures += [test_tuple]
    except status.TestFailure:
      test_tuple = (topology_name, int(time.time()) - start_secs)
      failures += [test_tuple]

    current += 1
  return successes, failures

def load_result_checker(topology_name, topology_conf,
                        expected_result_handler, actual_result_handler):
  # the task count setting controls is used to trigger the emit until spout wrapper, which is
  # currently only used in at least once tests. if that changes we need to expand our config
  # settings
  if "expectedHttpResultTaskCount" in topology_conf:
    return AtLeastOnceResultsChecker(
        topology_name, expected_result_handler, actual_result_handler)
  else:
    return ExactlyOnceResultsChecker(
        topology_name, expected_result_handler, actual_result_handler)

def load_expected_result_handler(topology_name, topology_conf, args, http_server_host_port):
  if "expectedResultRelativePath" in topology_conf:
    expected_result_file_path =\
      args.topologies_path + "/" + topology_conf["expectedResultRelativePath"]
    return FileBasedExpectedResultsHandler(expected_result_file_path)
  elif "expectedHttpResultTaskCount" in topology_conf:
    return HttpBasedExpectedResultsHandler(
        http_server_host_port, topology_name, topology_conf["expectedHttpResultTaskCount"])
  else:
    raise status.TestFailure("Either expectedResultRelativePath or expectedHttpResultTaskCount "
                             + "must be specified for test %s " % topology_name)

def main():
  ''' main '''
  log.configure(level=logging.DEBUG)
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
