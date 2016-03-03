#!/usr/bin/env python2.7

import argparse
import sys
import subprocess
import os
import tarfile
import contextlib
import getpass

# default environ tag, if not provided
ENVIRON = "default"

# directories for heron distribution
BIN_DIR  = "bin"
CONF_DIR = "conf"
ETC_DIR  = "etc"
LIB_DIR  = "lib"

# directories for heron sandbox
SANDBOX_CONF_DIR = "heron-conf"

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

def get_heron_cluster_conf_dir(cluster_role_env, default_config_path):
  """
  This will provide heron cluster config directory, if config path is default
  :return: absolute path of heron cluster conf directory
  """
  cluster_role_env = parse_cluster_role_env(cluster_role_env)
  if default_config_path == get_heron_conf_dir():
    return os.path.join(default_config_path, cluster_role_env[0])

  return default_config_path

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
# Parse the cluster/[role]/[environ], supply defaults, if not provided
################################################################################
def parse_cluster_role_env(cluster_role_env):
  parts = cluster_role_env.split('/')[:3]

  # if role is not provided, use username instead
  if len(parts) == 1:
    parts.append(getpass.getuser())

  # if environ is not provided, use 'default'
  if len(parts) == 2:
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
