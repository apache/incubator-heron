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

from ..common import status
from heron.common.src.python.utils import log
from heron.statemgrs.src.python import configloader
from heron.statemgrs.src.python.zkstatemanager import ZkStateManager
from heron.statemgrs.src.python.filestatemanager import FileStateManager

# The location of default configure file
DEFAULT_TEST_CONF_FILE = "integration_test/src/python/topology_test_runner/resources/test.json"

#seconds
RETRY_ATTEMPTS = 15
RETRY_INTERVAL = 10
WAIT_FOR_DEACTIVATION = 5


class TopologyStructureResultChecker(object):
  """
  Validate topology graph structure
  """
  def __init__(self, topology_name,
    topology_structure_expected_results_handler,
    topology_structure_actual_results_handler):
    self.topology_name = topology_name
    self.topology_structure_expected_results_handler = topology_structure_expected_results_handler
    self.topology_structure_actual_results_handler = topology_structure_actual_results_handler

  def check_results(self):
    """
    Checks the topology graph structure from zk with the expected results from local file
    """
    expected_result = self.topology_structure_expected_results_handler.fetch_results()
    actual_result = self.topology_structure_actual_results_handler.fetch_cur_pplan()

    self.topology_structure_actual_results_handler.stop_state_mgr()

    decoder = json.JSONDecoder(strict=False)
    expected_results = decoder.decode(expected_result)

    return self._compare(expected_results, actual_result)

  def _compare(self, expected_results, actual_results):
    """
    check if the topology structure is correct
    """
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
      raise status.TestFailure("Actual result did not match expected result")

  def _parse_expected_results(self, expected_results):
    """
    Parse JSON file and generate expected_nodes and expected_links
    """
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
    """
    Parse protobuf messege and generate actual_nodes and actual_links
    """
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


class InstanceStateResultChecker(TopologyStructureResultChecker):
  """"
  Validating instance states after checkpoint rollback
  TODO(yaoli): complete this class when stateful processing is ready
  """
  def __init__(self, topology_name,
    topology_structure_expected_results_handler,
    topology_structure_actual_results_handler,
    instance_state_expected_result_handler,
    instance_state_actual_result_handler):
    TopologyStructureResultChecker.__init__(self,topology_name,
      topology_structure_expected_results_handler,
      topology_structure_actual_results_handler)
    self.instance_state_expected_result_handler = instance_state_expected_result_handler
    self.instance_state_actual_result_handler = instance_state_actual_result_handler

  def check_results(self):
    topology_structure_check_result = TopologyStructureResultChecker.check_results(self)
    if isinstance(topology_structure_check_result, status.TestFailure):
      raise status.TestFailure("The actual topology graph structure does not match the expected one"
                               + " for topology: %s" % self.topology_name)
    # check instance states, get the instance_state_check_result
    # if both above are isinstanc(status.TestSuccess), return success, else return fail

  def _compare_state(self, expected_results, actual_results):
    pass

  def _parse_state_expected_results(self, expected_results):
    pass

  def _parse_state_actual_results(self, actual_results):
    pass


class FileBasedExpectedResultsHandler(object):
  """
  Get expected topology graph structure result from local file
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


class ZkFileBasedActualResultsHandler(object):
  """
  Get actual topology graph structure result from zk
  """
  def __init__(self, topology_name, cluster):
    self.topology_name = topology_name
    self.state_mgr = self._load_state_mgr(cluster)
    self.state_mgr.start()

  def _load_state_mgr(self, cluster):
    state_mgr_config = configloader.load_state_manager_locations(cluster, os.getenv("HOME")
                                                                 +'/.heron/conf/'+cluster
                                                                 + '/statemgr.yaml')
    if state_mgr_config[0]["type"] == 'file':
      return FileStateManager(self.topology_name, os.getenv("HOME")
                                                  +'/.herondata/repository/state/local')
    elif state_mgr_config[0]["type"] == 'zookeeper':
      host_port = state_mgr_config[0]["hostport"].split(':')
      return ZkStateManager(state_mgr_config[0]["type"],
        [(host_port[0], int(host_port[1]))],
        state_mgr_config[0]["rootpath"],
        state_mgr_config[0]["tunnelhost"])
    else:
      raise status.TestFailure("Unrecognized state manager type: %s"
                               % state_mgr_config["type"])

  def fetch_cur_pplan(self):
    try:
      for i in range(0, RETRY_ATTEMPTS):
        logging.info("Fetching physical plan of topology %s, retry count: %d", self.topology_name, i)
        try:
          pplan_string = self.state_mgr.get_pplan(self.topology_name)
        except IOError:
          pplan_string = None
        if pplan_string is not None and pplan_string.topology.state == 1: # RUNNING = 1
          break
        time.sleep(RETRY_INTERVAL)
      else:
        raise status.TestFailure("Fetching physical plan failed for %s topology"
                                 % self.topology_name)
      return pplan_string
    except Exception as e:
      raise status.TestFailure("Fetching physical plan failed for %s topology"
                               % self.topology_name, e)

  def stop_state_mgr(self):
    self.state_mgr.stop()


class HttpBasedActualResultsHandler(object):
  """
  Get actually loaded instance states
  TODO(yaoli): complete this class when stateful processing is ready
  """
  pass

class StatefulStorageBasedExpectedResultsHandler(object):
  """
  Get expected instance states from checkpoint storage
  TODO(yaoli): complete this class when stateful processing is ready
  """
  pass

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
  params, update_args, deactivate_args, restart_args, extra_topology_args):
  try:
    args = "-t %s %s" % \
           (topology_name, extra_topology_args)
    submit_topology(params.heron_cli_path, params.cli_config_path, params.cluster, params.role,
      params.env, params.tests_bin_path, classpath,
      params.release_package_uri, args)
  except Exception as e:
    raise status.TestFailure("Failed to submit %s topology" % topology_name, e)

  logging.info("Successfully submitted %s topology", topology_name)

  try:
    if update_args:
      # check if pplan is already available
      results_checker.topology_structure_actual_results_handler.fetch_cur_pplan()
      logging.info("Verified topology successfully started, proceeding to update it")
      update_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
        params.role, params.env, topology_name, update_args)
    elif deactivate_args:
      results_checker.topology_structure_actual_results_handler.fetch_cur_pplan()
      logging.info("Verified topology successfully started, proceeding "
                    + "to deactivate and activate it")
      deactivate_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
        params.role, params.env, topology_name, True)
      time.sleep(WAIT_FOR_DEACTIVATION)
      deactivate_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
        params.role, params.env, topology_name, False)
    elif restart_args:
      results_checker.topology_structure_actual_results_handler.fetch_cur_pplan()
      logging.info("Verified topology successfully started, proceeding to kill a container")
      restart_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
        params.role, params.env, topology_name, 1)

    return results_checker.check_results()

  except Exception as e:
    raise status.TestFailure("Checking result failed for %s topology" % topology_name, e)
  finally:
    kill_topology(params.heron_cli_path, params.cli_config_path, params.cluster,
      params.role, params.env, topology_name)
    pass


# Topology operations

def submit_topology(heron_cli_path, cli_config_path, cluster, role,
  env, jar_path, classpath, pkg_uri, args=None):
  """
  Submit topology using heron-cli
  """
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


def deactivate_topology(heron_cli_path, cli_config_path, cluster,
    role, env, topology_name, deactivate):
  if deactivate:
    cmd = "%s deactivate --config-path=%s %s %s" % \
          (heron_cli_path, cli_config_path,
          cluster_token(cluster, role, env), topology_name)
    logging.info("deactivate topology: %s", cmd)
    if os.system(cmd) != 0:
      raise status.TestFailure("Failed to deactivate topology %s" % topology_name)
    logging.info("Successfully deactivate topology %s", topology_name)
  else:
    cmd = "%s activate --config-path=%s %s %s" % \
          (heron_cli_path, cli_config_path,
          cluster_token(cluster, role, env), topology_name)
    logging.info("activate topology: %s", cmd)
    if os.system(cmd) != 0:
      raise status.TestFailure("Failed to activate topology %s" % topology_name)
    logging.info("Successfully activate topology %s", topology_name)


def restart_topology(heron_cli_path, cli_config_path, cluster,
    role, env, topology_name, container_id):
  cmd = "%s restart --config-path=%s %s %s %s" % \
        (heron_cli_path, cli_config_path,
        cluster_token(cluster, role, env), topology_name, str(container_id))

  logging.info("Kill container %s", cmd)
  if os.system(cmd) != 0:
    raise status.TestFailure("Failed to kill container %s" % str(container_id))

  logging.info("Successfully kill container %s", str(container_id))


def kill_topology(heron_cli_path, cli_config_path, cluster, role, env, topology_name):
  """
  Kill a topology using heron-cli
  """
  cmd = "%s kill --config-path=%s %s %s" % \
        (heron_cli_path, cli_config_path, cluster_token(cluster, role, env), topology_name)

  logging.info("Killing topology: %s", cmd)
  if os.system(cmd) != 0:
    raise status.TestFailure("Failed to kill topology %s" % topology_name)

  logging.info("Successfully killed topology %s", topology_name)


def cluster_token(cluster, role, env):
  if cluster == "local" or cluster == "localzk":
    return cluster
  return "%s/%s/%s" % (cluster, role, env)

#  Topology manipulations end


def run_topology_tests(conf, args):
  """
  Run the test for each topology specified in the conf file
  """
  successes = []
  failures = []
  timestamp = time.strftime('%Y%m%d%H%M%S')

  # http_host_port = "%s:%d" % (args.http_hostname, args.http_port)

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

    update_args = ""
    deactivate_args = ""
    restart_args = ""
    topology_args = ''
    if "updateArgs" in topology_conf:
      update_args = topology_conf["updateArgs"]
    if "deactivateArgs" in topology_conf:
      deactivate_args = True
    if "restartArgs" in topology_conf:
      restart_args = True

    if "topologyArgs" in topology_conf:
      topology_args = "%s %s" % (topology_args, topology_conf["topologyArgs"])

    expected_result_file_path = \
      args.topologies_path + "/" + topology_conf["expectedResultRelativePath"]
    check_type = topology_conf["checkType"]
    if check_type == 'topology_structure':
      results_checker = load_result_checker(
        check_type, topology_name,
        FileBasedExpectedResultsHandler(expected_result_file_path),
        ZkFileBasedActualResultsHandler(topology_name, args.cluster))
    elif check_type == 'checkpoint_state':
      if processing_type == 'stateful':
        results_checker = load_result_checker(
          check_type, topology_name,
          FileBasedExpectedResultsHandler(expected_result_file_path),
          ZkFileBasedActualResultsHandler(topology_name, args.cluster),
          StatefulStorageBasedExpectedResultsHandler(),
          HttpBasedActualResultsHandler())
      elif processing_type == 'non_stateful':
        raise ValueError("Cannot check instance checkpoint state in non_stateful processing. "
                       + "Not running topology: " + topology_name)
      else:
        raise ValueError("Unrecognized processing type for topology: " + topology_name)
    else:
      raise ValueError("Unrecognized check type for topology: " + topology_name)

    logging.info("==== Starting test %s of %s: %s ====",
      current, len(test_topologies), topology_name)
    start_secs = int(time.time())
    try:
      result = run_topology_test(topology_name, classpath, results_checker,
        args, update_args, deactivate_args, restart_args, topology_args)
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
  expected_topology_structure_result_handler,
  actual_topology_structure_result_handler,
  expected_instance_state_result_handler = None,
  actual_instance_state_result_handler = None):

  if check_type == "topology_structure":
    return TopologyStructureResultChecker(topology_name,
      expected_topology_structure_result_handler,
      actual_topology_structure_result_handler)
  elif check_type == "checkpoint_state":
    return InstanceStateResultChecker(topology_name,
      expected_topology_structure_result_handler,
      actual_topology_structure_result_handler,
      expected_instance_state_result_handler,
      actual_instance_state_result_handler)
  else:
    status.TestFailure("Unrecognized check type : %s", check_type)


def main():
  """
  main
  """
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
  parser.add_argument('-rh', '--http-hostname', dest='http_hostname', default='localhost')
  parser.add_argument('-rp', '--http-port', dest='http_port', type=int,
    default='8080')
  parser.add_argument('-tp', '--topologies-path', dest='topologies_path')
  parser.add_argument('-ts', '--test-topology-pattern', dest='test_topology_pattern', default=None)
  parser.add_argument('-pi', '--release-package-uri', dest='release_package_uri', default=None)
  parser.add_argument('-cd', '--cli-config-path', dest='cli_config_path',
    default=conf['cliConfigPath'])

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
