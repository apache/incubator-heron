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

#!/usr/bin/env python2.7

import argparse
import contextlib
import getpass
import os
import sys
import subprocess
import tarfile
import yaml

from heron.common.src.python.color import Log

# default environ tag, if not provided
ENVIRON = "default"

# directories for heron distribution
BIN_DIR  = "bin"
CONF_DIR = "conf"
ETC_DIR  = "etc"
LIB_DIR  = "lib"

# directories for heron sandbox
SANDBOX_CONF_DIR = "./heron-conf"

# config file for heron cli
CLIENT_YAML = "client.yaml"

# cli configs for role and env
ROLE_REQUIRED = "heron.config.role.required"
ENV_REQUIRED  = "heron.config.env.required"

################################################################################
# Create a tar file with a given set of files
################################################################################
def create_tar(tar_filename, files, config_dir):
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

################################################################################
# Retrieve the given subparser from parser
################################################################################
def get_subparser(parser, command):
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

################################################################################
# Get normalized class path depending on platform
################################################################################
def identity(x):
  return x

def cygpath(x):
  command = ['cygpath', '-wp', x]
  p = subprocess.Popen(command,stdout=subprocess.PIPE)
  output, errors = p.communicate()
  lines = output.split("\n")
  return lines[0]

def normalized_class_path(x):
  if sys.platform == 'cygwin':
    return cygpath(x)
  return identity(x)

################################################################################
# Get the normalized class path of all jars
################################################################################
def get_classpath(jars):
  return ':'.join(map(normalized_class_path, jars))

################################################################################
# Get the root of heron dir and various sub directories depending on platform
################################################################################
def get_heron_dir():
  """
  This will extract heron directory from .pex file.
  :return: root location for heron-cli.
  """
  path = "/".join(os.path.realpath( __file__ ).split('/')[:-7])
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

def get_heron_cluster_conf_dir(cluster, default_config_path):
  """
  This will provide heron cluster config directory, if config path is default
  :return: absolute path of heron cluster conf directory
  """
  return os.path.join(default_config_path, cluster)

################################################################################
# Get the sandbox directories and config files
################################################################################
def get_heron_sandbox_conf_dir():
  """
  This will provide heron conf directory in the sandbox
  :return: relative path of heron sandbox conf directory
  """
  return SANDBOX_CONF_DIR;

################################################################################
# Get all the heron lib jars with the absolute paths
################################################################################
def get_heron_libs(local_jars):
  heron_lib_dir = get_heron_lib_dir()
  heron_libs = [os.path.join(heron_lib_dir, f) for f in local_jars]
  return heron_libs

################################################################################
# Get the cluster to which topology is submitted
################################################################################
def get_heron_cluster(cluster_role_env):
  return cluster_role_env.split('/')[0]

################################################################################
# Parse cluster/[role]/[environ], supply default, if not provided, not required
################################################################################
def parse_cluster_role_env(cluster_role_env, config_path):
  parts = cluster_role_env.split('/')[:3]

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
      with open(cli_conf_file, 'r') as conf_file:
        cli_confs = yaml.load(conf_file)

        # if role is required but not provided, raise exception
        if len(parts) == 1:
          if (ROLE_REQUIRED in cli_confs) and (cli_confs[ROLE_REQUIRED] == True):
            raise Exception("role required but not provided")
          else:
            parts.append(getpass.getuser())

        # if environ is required but not provided, raise exception
        if len(parts) == 2:
          if (ENV_REQUIRED in cli_confs) and (cli_confs[ENV_REQUIRED] == True):
            raise Exception("environ required but not provided")
          else:
            parts.append(ENVIRON)

  # if cluster or role or environ is empty, print
  if len(parts[0]) == 0 or len(parts[1]) == 0 or len(parts[2]) == 0:
    print "Failed to parse %s: %s" % (argstr, namespace[argstr])
    sys.exit(1)

  return (parts[0], parts[1], parts[2])

################################################################################
# Parse the command line for overriding the defaults
################################################################################
def parse_cmdline_override(namespace):
  override = []
  for key in namespace.keys():
    # Notice we could not use "if not namespace[key]",
    # since it would filter out 0 too, rather than just "None"
    if namespace[key] is None:
      continue
    property_key = key.replace('-', '.').replace('_', '.')
    property_value = str(namespace[key])
    override.append('%s="%s"' % (property_key, property_value))
  return ' '.join(override)

################################################################################
# Get the path of java executable
################################################################################

def get_java_path():
  java_home = os.environ.get("JAVA_HOME")
  return os.path.join(java_home, BIN_DIR, "java")

################################################################################
# Check if the java home set
################################################################################
def check_java_home_set():

  # check if environ variable is set
  if not os.environ.has_key("JAVA_HOME"):
    Log.error("JAVA_HOME not set")
    return False

  # check if the value set is correct
  java_path = get_java_path()
  if os.path.isfile(java_path) and os.access(java_path, os.X_OK):
    return True

  Log.error("JAVA_HOME/bin/java either does not exist or not an executable")
  return False
