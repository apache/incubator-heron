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
''' utils.py '''
import argparse
import contextlib
import getpass
import os
import sys
import subprocess
import tarfile
import tempfile
import traceback
import tornado.gen
import tornado.ioloop
import yaml

from heron.common.src.python.color import Log
from heron.common.src.python.handler.access import heron as API

# default environ tag, if not provided
ENVIRON = "default"

# directories for heron distribution
BIN_DIR = "bin"
CONF_DIR = "conf"
ETC_DIR = "etc"
LIB_DIR = "lib"
CLI_DIR = ".heron"
RELEASE_YAML = "release.yaml"
OVERRIDE_YAML = "override.yaml"

# directories for heron sandbox
SANDBOX_CONF_DIR = "./heron-conf"

# config file for heron cli
CLIENT_YAML = "client.yaml"

# cli configs for role and env
IS_ROLE_REQUIRED = "heron.config.is.role.required"
IS_ENV_REQUIRED = "heron.config.is.env.required"


def create_tar(tar_filename, files, config_dir, config_files):
  '''
  Create a tar file with a given set of files
  '''
  with contextlib.closing(tarfile.open(tar_filename, 'w:gz')) as tar:
    for filename in files:
      if os.path.isfile(filename):
        tar.add(filename, arcname=os.path.basename(filename))
      else:
        raise Exception("%s is not an existing file" % filename)

    if os.path.isdir(config_dir):
      tar.add(config_dir, arcname=get_heron_sandbox_conf_dir())
    else:
      raise Exception("%s is not an existing directory" % config_dir)

    for filename in config_files:
      if os.path.isfile(filename):
        arcfile = os.path.join(get_heron_sandbox_conf_dir(), os.path.basename(filename))
        tar.add(filename, arcname=arcfile)
      else:
        raise Exception("%s is not an existing file" % filename)


def get_subparser(parser, command):
  '''
  Retrieve the given subparser from parser
  '''
  # pylint: disable=protected-access
  subparsers_actions = [
      action for action in parser._actions
      if isinstance(action, argparse._SubParsersAction)
  ]

  # there will probably only be one subparser_action,
  # but better save than sorry
  for subparsers_action in subparsers_actions:
    # get all subparsers
    for choice, subparser in subparsers_action.choices.items():
      if choice == command:
        return subparser
  return None


def cygpath(x):
  '''
  normalized class path on cygwin
  '''
  command = ['cygpath', '-wp', x]
  p = subprocess.Popen(command, stdout=subprocess.PIPE)
  result = p.communicate()
  output = result[0]
  lines = output.split("\n")
  return lines[0]


def identity(x):
  '''
  identity function
  '''
  return x


def normalized_class_path(x):
  '''
  normalize path
  '''
  if sys.platform == 'cygwin':
    return cygpath(x)
  return identity(x)


def get_classpath(jars):
  '''
  Get the normalized class path of all jars
  '''
  return ':'.join(map(normalized_class_path, jars))


def get_heron_dir():
  """
  This will extract heron directory from .pex file.
  :return: root location for heron-cli.
  """
  path = "/".join(os.path.realpath(__file__).split('/')[:-7])
  return normalized_class_path(path)


################################################################################
# Get the root of heron dir and various sub directories depending on platform
################################################################################
def get_heron_dir_explorer():
  """
  This will extract heron directory from .pex file.
  From heron-cli with modification since we need to reuse cli's conf
  :return: root location for heron-cli.
  """
  path_list = os.path.realpath(__file__).split('/')[:-8]
  path_list.append(CLI_DIR)
  path = "/".join(path_list)
  return normalized_class_path(path)


def get_heron_bin_dir():
  """
  This will provide heron bin directory from .pex file.
  :return: absolute path of heron lib directory
  """
  bin_path = os.path.join(get_heron_dir(), BIN_DIR)
  return bin_path


def get_heron_conf_dir():
  """
  This will provide heron conf directory from .pex file.
  :return: absolute path of heron conf directory
  """
  conf_path = os.path.join(get_heron_dir(), CONF_DIR)
  return conf_path


def get_heron_lib_dir():
  """
  This will provide heron lib directory from .pex file.
  :return: absolute path of heron lib directory
  """
  lib_path = os.path.join(get_heron_dir(), LIB_DIR)
  return lib_path


def get_heron_release_file():
  """
  This will provide the path to heron release.yaml file
  :return: absolute path of heron release.yaml file
  """
  return os.path.join(get_heron_dir(), RELEASE_YAML)


def get_heron_cluster_conf_dir(cluster, default_config_path):
  """
  This will provide heron cluster config directory, if config path is default
  :return: absolute path of heron cluster conf directory
  """
  return os.path.join(default_config_path, cluster)


def get_heron_sandbox_conf_dir():
  """
  This will provide heron conf directory in the sandbox
  :return: relative path of heron sandbox conf directory
  """
  return SANDBOX_CONF_DIR


def get_heron_libs(local_jars):
  """Get all the heron lib jars with the absolute paths"""
  heron_lib_dir = get_heron_lib_dir()
  heron_libs = [os.path.join(heron_lib_dir, f) for f in local_jars]
  return heron_libs


def get_heron_cluster(cluster_role_env):
  """Get the cluster to which topology is submitted"""
  return cluster_role_env.split('/')[0]


# pylint: disable=too-many-branches
def parse_cluster_role_env(cluster_role_env, config_path):
  """Parse cluster/[role]/[environ], supply default, if not provided, not required"""
  parts = cluster_role_env.split('/')[:3]
  Log.info("Using config file under %s" % config_path)
  if not os.path.isdir(config_path):
    Log.error("Config path cluster directory does not exist: %s" % config_path)
    raise Exception("Invalid config path")

  # if cluster/role/env is not completely provided, check further
  if len(parts) < 3:

    cli_conf_file = os.path.join(config_path, CLIENT_YAML)

    # if client conf doesn't exist, use default value
    if not os.path.isfile(cli_conf_file):
      if len(parts) == 1:
        parts.append(getpass.getuser())
      if len(parts) == 2:
        parts.append(ENVIRON)
    else:
      cli_confs = {}
      with open(cli_conf_file, 'r') as conf_file:
        tmp_confs = yaml.load(conf_file)
        # the return value of yaml.load can be None if conf_file is an empty file
        if tmp_confs is not None:
          cli_confs = tmp_confs
        else:
          print "Failed to read: %s due to it is empty" % (CLIENT_YAML)

      # if role is required but not provided, raise exception
      if len(parts) == 1:
        if (IS_ROLE_REQUIRED in cli_confs) and (cli_confs[IS_ROLE_REQUIRED] is True):
          raise Exception(
              "role required but not provided (cluster/role/env = %s). See %s in %s" %
              (cluster_role_env, IS_ROLE_REQUIRED, CLIENT_YAML))
        else:
          parts.append(getpass.getuser())

      # if environ is required but not provided, raise exception
      if len(parts) == 2:
        if (IS_ENV_REQUIRED in cli_confs) and (cli_confs[IS_ENV_REQUIRED] is True):
          raise Exception(
              "environ required but not provided (cluster/role/env = %s). See %s in %s" %
              (cluster_role_env, IS_ENV_REQUIRED, CLIENT_YAML))
        else:
          parts.append(ENVIRON)

  # if cluster or role or environ is empty, print
  if len(parts[0]) == 0 or len(parts[1]) == 0 or len(parts[2]) == 0:
    print "Failed to parse"
    sys.exit(1)

  return (parts[0], parts[1], parts[2])


################################################################################
# Parse the command line for overriding the defaults
################################################################################
def parse_override_config(namespace):
  """Parse the command line for overriding the defaults"""
  try:
    tmp_dir = tempfile.mkdtemp()
    override_config_file = os.path.join(tmp_dir, OVERRIDE_YAML)
    with open(override_config_file, 'w') as f:
      for config in namespace:
        f.write("%s\n" % config.replace('=', ': '))

    return override_config_file
  except Exception as e:
    raise Exception("Failed to parse override config: %s" % str(e))


def get_java_path():
  """Get the path of java executable"""
  java_home = os.environ.get("JAVA_HOME")
  return os.path.join(java_home, BIN_DIR, "java")


def check_java_home_set():
  """Check if the java home set"""
  # check if environ variable is set
  if "JAVA_HOME" not in os.environ:
    Log.error("JAVA_HOME not set")
    return False

  # check if the value set is correct
  java_path = get_java_path()
  if os.path.isfile(java_path) and os.access(java_path, os.X_OK):
    return True

  Log.error("JAVA_HOME/bin/java either does not exist or not an executable")
  return False


def check_release_file_exists():
  """Check if the release.yaml file exists"""
  release_file = get_heron_release_file()

  # if the file does not exist and is not a file
  if not os.path.isfile(release_file):
    Log.error("Required file not found: %s" % release_file)
    return False

  return True


def _all_metric_queries():
  queries_normal = [
      'complete-latency', 'execute-latency', 'process-latency',
      'jvm-uptime-secs', 'jvm-process-cpu-load', 'jvm-memory-used-mb'
  ]
  queries = ['__%s' % m for m in queries_normal]
  count_queries_normal = ['emit-count', 'execute-count', 'ack-count', 'fail-count']
  count_queries = ['__%s/default' % m for m in count_queries_normal]
  return queries, queries_normal, count_queries, count_queries_normal


def metric_queries():
  """all metric queries"""
  qs = _all_metric_queries()
  return qs[0] + qs[2]


def queries_map():
  """map from query parameter to query name"""
  qs = _all_metric_queries()
  return dict(zip(qs[0], qs[1]) + zip(qs[2], qs[3]))


def get_clusters():
  """Synced API call to get all cluster names"""
  instance = tornado.ioloop.IOLoop.instance()
  # pylint: disable=unnecessary-lambda
  try:
    return instance.run_sync(lambda: API.get_clusters())
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_logical_plan(cluster, env, topology, role):
  """Synced API call to get logical plans"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_logical_plan(cluster, env, topology, role))
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_topology_info(*args):
  """Synced API call to get topology information"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_topology_info(*args))
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_topology_metrics(*args):
  """Synced API call to get topology metrics"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_comp_metrics(*args))
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_component_metrics(component, cluster, env, topology, role):
  """Synced API call to get component metrics"""
  all_queries = metric_queries()
  try:
    result = get_topology_metrics(
        cluster, env, topology, component, [], all_queries, [0, -1], role)
    return result["metrics"]
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_cluster_topologies(cluster):
  """Synced API call to get topologies under a cluster"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_topologies(cluster))
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_cluster_role_topologies(cluster, role):
  """Synced API call to get topologies under a cluster submitted by a role"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_role_topologies(cluster, role))
  except Exception:
    Log.info(traceback.format_exc())
    raise


def get_cluster_role_env_topologies(cluster, role, env):
  """Synced API call to get topologies under a cluster submitted by a role under env"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_role_env_topologies(cluster, role, env))
  except Exception:
    Log.info(traceback.format_exc())
    raise

def print_version():
  release_file = get_heron_release_file()
  with open(release_file) as release_info:
    for line in release_info:
      print line,
