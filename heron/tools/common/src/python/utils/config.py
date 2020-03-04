#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

'''config.py: util functions for config, mainly for heron-cli'''

import argparse
import contextlib
import getpass
import os
import sys
import subprocess
import tarfile
import tempfile
import yaml

from heron.common.src.python.utils.log import Log

# pylint: disable=logging-not-lazy

# default environ tag, if not provided
ENVIRON = "default"

# directories for heron distribution
BIN_DIR = "bin"
CONF_DIR = "conf"
ETC_DIR = "etc"
LIB_DIR = "lib"
CLI_DIR = ".heron"
RELEASE_YAML = "release.yaml"
ZIPPED_RELEASE_YAML = "scripts/packages/release.yaml"
OVERRIDE_YAML = "override.yaml"

# mode of deployment
DIRECT_MODE = 'direct'
SERVER_MODE = 'server'

# directories for heron sandbox
SANDBOX_CONF_DIR = "./heron-conf"

# config file for heron cli
CLIENT_YAML = "client.yaml"

# client configs for role and env for direct deployment
ROLE_REQUIRED = "heron.config.is.role.required"
ENV_REQUIRED = "heron.config.is.env.required"

# client config for role and env for server deployment
ROLE_KEY = "role.required"
ENVIRON_KEY = "env.required"

def create_tar(tar_filename, files, config_dir, config_files):
  '''
  Create a tar file with a given set of files
  '''
  with contextlib.closing(tarfile.open(tar_filename, 'w:gz', dereference=True)) as tar:
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
  subparsers_actions = [action for action in parser._actions
                        if isinstance(action, argparse._SubParsersAction)]

  # there will probably only be one subparser_action,
  # but better save than sorry
  for subparsers_action in subparsers_actions:
    # get all subparsers
    for choice, subparser in list(subparsers_action.choices.items()):
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

  For example,
  when __file__ is '/Users/heron-user/bin/heron/heron/tools/common/src/python/utils/config.pyc', and
  its real path is '/Users/heron-user/.heron/bin/heron/tools/common/src/python/utils/config.pyc',
  the internal variable ``path`` would be '/Users/heron-user/.heron', which is the heron directory

  This means the variable `go_above_dirs` below is 9.

  :return: root location of the .pex file
  """
  go_above_dirs = 9
  path = "/".join(os.path.realpath(__file__).split('/')[:-go_above_dirs])
  return normalized_class_path(path)

def get_zipped_heron_dir():
  """
  This will extract heron directory from .pex file,
  with `zip_safe = False' Bazel flag added when building this .pex file

  For example,
  when __file__'s real path is
    '/Users/heron-user/.pex/code/xxxyyy/heron/tools/common/src/python/utils/config.pyc', and
  the internal variable ``path`` would be '/Users/heron-user/.pex/code/xxxyyy/',
  which is the root PEX directory

  This means the variable `go_above_dirs` below is 7.

  :return: root location of the .pex file.
  """
  go_above_dirs = 7
  path = "/".join(os.path.realpath(__file__).split('/')[:-go_above_dirs])
  return normalized_class_path(path)

################################################################################
# Get the root of heron dir and various sub directories depending on platform
################################################################################
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
  :return: absolute path of heron release.yaml file in CLI
  """
  return os.path.join(get_heron_dir(), RELEASE_YAML)


def get_zipped_heron_release_file():
  """
  This will provide the path to heron release.yaml file.
  To be used for .pex file built with `zip_safe = False` flag.
  For example, `heron-ui'.

  :return: absolute path of heron release.yaml file
  """
  return os.path.join(get_zipped_heron_dir(), ZIPPED_RELEASE_YAML)


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

################################################################################
# pylint: disable=too-many-branches,superfluous-parens
def parse_cluster_role_env(cluster_role_env, config_path):
  """Parse cluster/[role]/[environ], supply default, if not provided, not required"""
  parts = cluster_role_env.split('/')[:3]
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
          print("Failed to read: %s due to it is empty" % (CLIENT_YAML))

      # if role is required but not provided, raise exception
      if len(parts) == 1:
        if (ROLE_REQUIRED in cli_confs) and (cli_confs[ROLE_REQUIRED] is True):
          raise Exception("role required but not provided (cluster/role/env = %s). See %s in %s"
                          % (cluster_role_env, ROLE_REQUIRED, cli_conf_file))
        else:
          parts.append(getpass.getuser())

      # if environ is required but not provided, raise exception
      if len(parts) == 2:
        if (ENV_REQUIRED in cli_confs) and (cli_confs[ENV_REQUIRED] is True):
          raise Exception("environ required but not provided (cluster/role/env = %s). See %s in %s"
                          % (cluster_role_env, ENV_REQUIRED, cli_conf_file))
        else:
          parts.append(ENVIRON)

  # if cluster or role or environ is empty, print
  if len(parts[0]) == 0 or len(parts[1]) == 0 or len(parts[2]) == 0:
    print("Failed to parse")
    sys.exit(1)

  return (parts[0], parts[1], parts[2])

################################################################################
def get_cluster_role_env(cluster_role_env):
  """Parse cluster/[role]/[environ], supply empty string, if not provided"""
  parts = cluster_role_env.split('/')[:3]
  if len(parts) == 3:
    return (parts[0], parts[1], parts[2])

  if len(parts) == 2:
    return (parts[0], parts[1], "")

  if len(parts) == 1:
    return (parts[0], "", "")

  return ("", "", "")

################################################################################
def direct_mode_cluster_role_env(cluster_role_env, config_path):
  """Check cluster/[role]/[environ], if they are required"""

  # otherwise, get the client.yaml file
  cli_conf_file = os.path.join(config_path, CLIENT_YAML)

  # if client conf doesn't exist, use default value
  if not os.path.isfile(cli_conf_file):
    return True

  client_confs = {}
  with open(cli_conf_file, 'r') as conf_file:
    client_confs = yaml.load(conf_file)

    # the return value of yaml.load can be None if conf_file is an empty file
    if not client_confs:
      return True

    # if role is required but not provided, raise exception
    role_present = True if len(cluster_role_env[1]) > 0 else False
    if ROLE_REQUIRED in client_confs and client_confs[ROLE_REQUIRED] and not role_present:
      raise Exception("role required but not provided (cluster/role/env = %s). See %s in %s"
                      % (cluster_role_env, ROLE_REQUIRED, cli_conf_file))

    # if environ is required but not provided, raise exception
    environ_present = True if len(cluster_role_env[2]) > 0 else False
    if ENV_REQUIRED in client_confs and client_confs[ENV_REQUIRED] and not environ_present:
      raise Exception("environ required but not provided (cluster/role/env = %s). See %s in %s"
                      % (cluster_role_env, ENV_REQUIRED, cli_conf_file))

  return True

################################################################################
def server_mode_cluster_role_env(cluster_role_env, config_map):
  """Check cluster/[role]/[environ], if they are required"""

  cmap = config_map[cluster_role_env[0]]

  # if role is required but not provided, raise exception
  role_present = True if len(cluster_role_env[1]) > 0 else False
  if ROLE_KEY in cmap and cmap[ROLE_KEY] and not role_present:
    raise Exception("role required but not provided (cluster/role/env = %s)."\
        % (cluster_role_env))

  # if environ is required but not provided, raise exception
  environ_present = True if len(cluster_role_env[2]) > 0 else False
  if ENVIRON_KEY in cmap and cmap[ENVIRON_KEY] and not environ_present:
    raise Exception("environ required but not provided (cluster/role/env = %s)."\
        % (cluster_role_env))

  return True

################################################################################
def defaults_cluster_role_env(cluster_role_env):
  """
  if role is not provided, supply userid
  if environ is not provided, supply 'default'
  """
  if len(cluster_role_env[1]) == 0 and len(cluster_role_env[2]) == 0:
    return (cluster_role_env[0], getpass.getuser(), ENVIRON)

  return (cluster_role_env[0], cluster_role_env[1], cluster_role_env[2])

################################################################################
# Parse the command line for overriding the defaults
################################################################################
def parse_override_config_and_write_file(namespace):
  """
  Parse the command line for overriding the defaults and
  create an override file.
  """
  overrides = parse_override_config(namespace)
  try:
    tmp_dir = tempfile.mkdtemp()
    override_config_file = os.path.join(tmp_dir, OVERRIDE_YAML)
    with open(override_config_file, 'w') as f:
      f.write(yaml.dump(overrides))

    return override_config_file
  except Exception as e:
    raise Exception("Failed to parse override config: %s" % str(e))


def parse_override_config(namespace):
  """Parse the command line for overriding the defaults"""
  overrides = dict()
  for config in namespace:
    kv = config.split("=")
    if len(kv) != 2:
      raise Exception("Invalid config property format (%s) expected key=value" % config)
    if kv[1] in ['true', 'True', 'TRUE']:
      overrides[kv[0]] = True
    elif kv[1] in ['false', 'False', 'FALSE']:
      overrides[kv[0]] = False
    else:
      overrides[kv[0]] = kv[1]
  return overrides


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

def print_build_info(zipped_pex=False):
  """Print build_info from release.yaml

  :param zipped_pex: True if the PEX file is built with flag `zip_safe=False'.
  """
  if zipped_pex:
    release_file = get_zipped_heron_release_file()
  else:
    release_file = get_heron_release_file()

  with open(release_file) as release_info:
    release_map = yaml.load(release_info)
    release_items = sorted(list(release_map.items()), key=lambda tup: tup[0])
    for key, value in release_items:
      print("%s : %s" % (key, value))

def get_version_number(zipped_pex=False):
  """Print version from release.yaml

  :param zipped_pex: True if the PEX file is built with flag `zip_safe=False'.
  """
  if zipped_pex:
    release_file = get_zipped_heron_release_file()
  else:
    release_file = get_heron_release_file()
  with open(release_file) as release_info:
    for line in release_info:
      trunks = line[:-1].split(' ')
      if trunks[0] == 'heron.build.version':
        return trunks[-1].replace("'", "")
    return 'unknown'


def insert_bool(param, command_args):
  '''
  :param param:
  :param command_args:
  :return:
  '''
  index = 0
  found = False
  for lelem in command_args:
    if lelem == '--' and not found:
      break
    if lelem == param:
      found = True
      break
    index = index + 1

  if found:
    command_args.insert(index + 1, 'True')
  return command_args


def insert_bool_values(command_line_args):
  '''
  :param command_line_args:
  :return:
  '''
  args1 = insert_bool('--verbose', command_line_args)
  args2 = insert_bool('--deploy-deactivated', args1)
  return args2

class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  def _format_action(self, action):
    # pylint: disable=bad-super-call
    parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts
