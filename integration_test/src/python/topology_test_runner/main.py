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
from heron.proto import tmaster_pb2, physical_plan_pb2

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "integration_test/src/python/topology_test_runner/resources/test.json"

RETRY_ATTEMPTS = 15
#secondsf
RETRY_INTERVAL = 10
# seconds
WAITING_FOR_UPDATE = 5


class TopologyStructureResultChecker(object):
  """Compares what results we found against what was expected. Verifies and exact match"""

  def __init__(self, topology_name, expected_results_handler, actual_results_handler):
    self.topology_name = topology_name
    self.expected_results_handler = expected_results_handler
    self.actual_results_handler = actual_results_handler

  def check_results(self):
    """ Checks the topology resul
    ts from the server with the expected results from the file """
    expected_result = self.expected_results_handler.fetch_results()
    actual_result = self.actual_results_handler.fetch_cur_pplan()

    pplan_string = self._decode_string(actual_result)

    with open("/Users/yaoli/test_pplan", 'w') as f:
          f.write(pplan_string)

    # Build a new instance of json decoder since the default one could not ignore "\n"
    decoder = json.JSONDecoder(strict=False)
    expected_results = decoder.decode(expected_result)
    logging.info("Got expected results")

    cur_pplan = physical_plan_pb2.PhysicalPlan()
    cur_pplan.ParseFromString(pplan_string)

    return self._compare(expected_results, cur_pplan)

  def _compare(self, expected_results, actual_results):
    '''check if the topology structure is correct'''
    expected_nodes, expected_links = self._parse_expected_results(expected_results)
    actual_nodes, actual_links = self._parse_actual_results(actual_results)
    correct_topology = True
    if correct_topology:
      for key_expected in expected_nodes:
        if key_expected not in actual_nodes:
          correct_topology = False
          break
        if expected_nodes[key_expected] != actual_nodes[key_expected]:
          correct_topology = False
          break
    if correct_topology:
      for key_actual in actual_nodes:
        if key_actual not in expected_nodes:
          correct_topology = False
          break
        if expected_nodes[key_actual] != actual_nodes[key_actual]:
          correct_topology = False
          break
    if correct_topology:
      for key_expected in expected_links:
        if key_expected not in actual_links:
          correct_topology = False
          break
        if expected_links[key_expected] != actual_links[key_expected]:
          correct_topology = False
          break
    if correct_topology:
      for key_actual in actual_links:
        if key_actual not in expected_links:
          correct_topology = False
          break
        if expected_links[key_actual] != actual_links[key_actual]:
          correct_topology = False
          break

    if correct_topology:
      return status.TestSuccess(
        "Topology %s result matches expected result" % self.topology_name)
    else:
      failure = status.TestFailure("Actual result did not match expected result")
      raise failure

  def _parse_expected_results(self, expected_results):
    """parse JSON file and generate expected_nodes and expected_links"""
    expected_nodes = dict()
    expected_links = dict()
    for bolt in expected_results["topology"]["bolts"]:
      name = bolt["comp"]["name"]
      if name not in expected_links:
        expected_links[name] = set()
      for input in bolt["inputs"]:
        expected_links[name].add(input["stream"]["component_name"])
    for instance in expected_results["instances"]:
      name = instance["info"]["component_name"]
      if name not in expected_nodes:
        expected_nodes[name] = 0
      else:
        expected_nodes[name] += 1

    return  expected_nodes, expected_links

  def _parse_actual_results(self, actual_results):
    """parse protobuf messege and generate actual_nodes and actual_links"""
    actual_nodes = dict()
    actual_links = dict()
    for bolt in actual_results.topology.bolts:
      name = bolt.comp.name
      if name not in actual_links:
        actual_links[name] = set()
      for input in bolt.inputs:
        actual_links[name].add(input.stream.component_name)
    for instance in actual_results.instances:
      name = instance.info.component_name
      if name not in actual_nodes:
        actual_nodes[name] = 0
      else:
        actual_nodes[name] += 1

    return actual_nodes, actual_links

  def _decode_string(self, actual_result):
    '''convert received pplan string to original string'''
    pplan_string_fixed = bytearray(actual_result)
    pplan_string = bytearray('')
    i = 0
    while i < len(pplan_string_fixed):
      chr_1 = pplan_string_fixed[i]
      if chr_1 <= 57:
        chr_1 -= 48
      else:
        chr_1 -= 55
      chr_2 = pplan_string_fixed[i+1]
      if chr_2 <= 57:
        chr_2 -= 48
      else:
        chr_2 -= 55
      i += 2
      new_chr = ((chr_1 << 4) + chr_2) & 0xFF
      pplan_string.append(new_chr)

    return bytes(pplan_string)


class FileBasedExpectedResultsHandler(object):
  """
  Get expected result from local files
  """
  def __init__(self, file_path):
    self.file_path = file_path

  def fetch_results(self):
    """
    Read expected result from the expected result file
    """
    try:
      if not os.path.exists(self.file_path):
        raise status.TestFailure("Expected results file %s does not exist" % self.file_path)
      else:
        with open(self.file_path, "r") as expected_result_file:
          return expected_result_file.read().rstrip()
    except Exception as e:
      raise status.TestFailure("Failed to read expected result file %s" % self.file_path, e)

class HttpBasedActualResultsHandler(object):
  """
  Get actual results from Tmaster through HTTP request
  """
  def __init__(self, zk_host_port, topology_name, cluster_token):
    self.zk_host_port = zk_host_port
    self.topology_name = topology_name
    self.cluster_token = cluster_token
    #self.get_tmaster_host_port()

  def get_tmaster_host_port(self):
    """
    For local test, get Tmaster's host and port from
        /Users/yaoli/.herondata/repository/state/local/tmasters
    """
    local_tmaster_path = "/Users/yaoli/.herondata/repository/state/local/tmasters/" \
                         + self.topology_name
    with open(local_tmaster_path, "r") as tmaster_location_file:
      tmaster_location_string = tmaster_location_file.read().rstrip()
    tmaster_location = tmaster_pb2.TMasterLocation()
    tmaster_location.ParseFromString(tmaster_location_string)
    self.tmaster_host_port = "%s:%d" % (tmaster_location.host, tmaster_location.controller_port)
    logging.info("Get Tmaster host and port from local file, host:port = %s"
                 % self.tmaster_host_port)

    #  Get TMaster host and port from zk
    '''zk = KazooClient(hosts='localhost:2181')
    zk.start()
    if zk.exists("/heron/tmasters/" + self.topology_name):
      data, stat = zk.get("/heron/tmasters/" + self.topology_name)
    tmaster_location = tmaster_pb2.TMasterLocation()
    tmaster_location.ParseFromString(data)
    self.tmaster_host_port = "%s:%d" % (tmaster_location.host, tmaster_location.controller_port)
    logging.info("Get Tmaster host and port from local file, host:port = %s"
                 % self.tmaster_host_port)'''

  def fetch_cur_pplan(self):
    try:
      return self.fetch_from_server('physicalPlan', '/get_current_physical_plan')
    except Exception as e:
      raise status.TestFailure("Fetching physical plan failed for %s topology"
                               % self.topology_name, e)

  def fetch_from_server(self, data_name, path):
    """
    Make a http get request to fetch actual results from Tmaster
    """
    for i in range(0, RETRY_ATTEMPTS):
      logging.info("Fetching %s for topology %s, retry count: %d", data_name, self.topology_name, i)
      response = self.get_http_response(path)
      if response.status == 200:
        #return response.read().rstrip()
        #logging.info('type of content length = %s' % type(response.getheader('Content-Length')))
        #logging.info('http response content length = %s' % response.getheader('Content-Length'))
        return response.read(int(response.getheader('Content-Length'))*2)
      elif i != RETRY_ATTEMPTS:
        logging.info("Fetching %s failed with status: %s; reason: %s; body: %s",
          data_name, response.status, response.reason, response.read())
        time.sleep(RETRY_INTERVAL)

    raise status.TestFailure("Failed to fetch %s after %d attempts" % (data_name, RETRY_ATTEMPTS))

  def get_http_response(self, path):
    """
    get HTTP response
    """
    for _ in range(0, RETRY_ATTEMPTS):
      try:
        connection = HTTPConnection(self.tmaster_host_port)
        connection.request('GET', path)
        response = connection.getresponse()
        logging.info('http response content length = %s' % response.getheader('Content-Length'))
        return response
      except Exception:
        time.sleep(RETRY_INTERVAL)
        continue

    raise status.TestFailure("Failed to get HTTP Response after %d attempts" % RETRY_ATTEMPTS)

#  Result handlers end


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


def run_topology_test(topology_name, classpath, results_checker,
  params, update_args, extra_topology_args):
  try:
    args = "-t %s %s" % \
           (topology_name, extra_topology_args)
    submit_topology(params.heron_cli_path, params.cli_config_path, params.cluster, params.role,
      params.env, params.tests_bin_path, classpath,
      params.release_package_uri, args)
  except Exception as e:
    raise status.TestFailure("Failed to submit %s topology" % topology_name, e)

  logging.info("Successfully submitted %s topology", topology_name)

  time.sleep(WAITING_FOR_UPDATE)
  try:
    if update_args:
      # check if pplan is already available
      results_checker.actual_results_handler.get_tmaster_host_port()
      results_checker.actual_results_handler.fetch_cur_pplan()
      # wait for some time, since pplan available at Tmaster does not mean the topology is ready
      time.sleep(WAITING_FOR_UPDATE)
      logging.info("Verified topology successfully started, proceeding to update it")
      update_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
        params.role, params.env, topology_name, update_args)
      time.sleep(WAITING_FOR_UPDATE)

    return results_checker.check_results()

  except Exception as e:
    raise status.TestFailure("Checking result failed for %s topology" % topology_name, e)
  finally:
    kill_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
      params.role, params.env, topology_name)
    pass


# Topology manipulations

def submit_topology(heron_cli_path, cli_config_path, cluster, role,
  env, jar_path, classpath, pkg_uri, args=None):
  ''' Submit topology using heron-cli '''
  # Form the command to submit a topology.
  # cmd = "%s submit localzk %s %s %s %s" % \
  cmd = "%s submit --config-path=%s %s %s %s %s" % \
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env),
        jar_path, classpath, args)

  if pkg_uri is not None:
    cmd = "%s --config-property heron.package.core.uri='%s'" %(cmd, pkg_uri)

  logging.info("Submitting topology: %s", cmd)

  if os.system(cmd) != 0:
    raise status.TestFailure("Unable to submit the topology")


def update_topology(heron_cli_path, cli_config_path, cluster,
  role, env, topology_name, update_args):
  cmd = "%s update --config-path=%s %s %s %s --verbose" % \
        (heron_cli_path, cli_config_path,
        cluster_token(cluster, role, env), update_args, topology_name)

  logging.info("Update topology: %s", cmd)
  if os.system(cmd) != 0:
    raise status.TestFailure("Failed to update topology %s" % topology_name)

  logging.info("Successfully updated topology %s", topology_name)


def kill_topology(heron_cli_path, cli_config_path, cluster, role, env, topology_name):
  ''' Kill a topology using heron-cli '''
  cmd = "%s kill --config-path=%s %s %s" % \
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env), topology_name)

  logging.info("Killing topology: %s", cmd)
  if os.system(cmd) != 0:
    raise status.TestFailure("Failed to kill topology %s" % topology_name)

  logging.info("Successfully killed topology %s", topology_name)


def cluster_token(cluster, role, env):
  # !!! For local test switching between using or not using zk
  if cluster == "local":
    return cluster
  return "%s/%s/%s" % (cluster, role, env)

#  Topology manipulations end


def run_topology_tests(conf, args):
  ''' Run the test for each topology specified in the conf file '''
  successes = []
  failures = []
  timestamp = time.strftime('%Y%m%d%H%M%S')

  zk_host_port = "%s:%d" % (args.zk_hostname, args.zk_port)

  if args.tests_bin_path.endswith("scala-integration-tests.jar"):
    test_topologies = filter_test_topologies(conf["scalaTopologies"], args.test_topology_pattern)
    topology_classpath_prefix = conf["topologyClasspathPrefix"]
  elif args.tests_bin_path.endswith("integration-topology-tests.jar"):
    test_topologies = filter_test_topologies(conf["javaTopologies"], args.test_topology_pattern)
    topology_classpath_prefix = conf["topologyClasspathPrefix"]
  elif args.tests_bin_path.endswith("heron_integ_topology.pex"):
    test_topologies = filter_test_topologies(conf["pythonTopologies"], args.test_topology_pattern)
    topology_classpath_prefix = ""
  else:
    raise ValueError("Unrecognized binary file type: %s" % args.tests_bin_path)

  processing_type = conf["processingType"]

  current = 1
  for topology_conf in test_topologies:
    topology_name = ("%s_%s_%s") % (timestamp, topology_conf["topologyName"], str(uuid.uuid4()))
    classpath = topology_classpath_prefix + topology_conf["classPath"]

    # if the test includes an update we need to pass that info to the topology so it can send
    # data accordingly. This flag causes the test spout to emit, then check the state of this
    # token, then emit more.
    update_args = ""
    topology_args = ''
    if "updateArgs" in topology_conf:
      update_args = topology_conf["updateArgs"]

    if "topologyArgs" in topology_conf:
      topology_args = "%s %s" % (topology_args, topology_conf["topologyArgs"])

    expected_result_file_path = \
      args.topologies_path + "/" + topology_conf["expectedResultRelativePath"]
    check_type = topology_conf["checkType"]
    if processing_type == 'non_stateful' and check_type == 'checkpoint_state':
      raise ValueError("Cannot check checkpoint state in non_stateful processing. "
                       + "Not running topology: " + topology_name)
    results_checker = load_result_checker(
        check_type, topology_name,
        FileBasedExpectedResultsHandler(expected_result_file_path),
        HttpBasedActualResultsHandler(zk_host_port, topology_name, cluster_token(
          args.cluster, args.role, args.env)))


    logging.info("==== Starting test %s of %s: %s ====",
      current, len(test_topologies), topology_name)
    start_secs = int(time.time())
    try:
      result = run_topology_test(topology_name, classpath, results_checker,
        args, update_args, topology_args)
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


def load_result_checker(check_type, topology_name,
  expected_result_handler, actual_result_handler):

  if check_type == "topology_structure":
    return TopologyStructureResultChecker(
        topology_name, expected_result_handler, actual_result_handler)
  elif check_type == "checkpoint_state":
    # TODO: stateful recover check
    pass
  else:
    status.TestFailure("Unrecognized check type : %s", check_type)





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
  parser.add_argument('-rh', '--zk-hostname', dest='zk_hostname', default='localhost')
  parser.add_argument('-rp', '--zk-port', dest='zk_port', type=int,
    default='2181')
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

  (successes, failures) = run_topology_tests(conf, args)
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
