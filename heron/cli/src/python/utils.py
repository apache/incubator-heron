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
import yaml

from heron.common.src.python.color import Log

# default environ tag, if not provided
ENVIRON = "default"

# directories for heron distribution
BIN_DIR = "bin"
CONF_DIR = "conf"
ETC_DIR = "etc"
LIB_DIR = "lib"
RELEASE_YAML = "release.yaml"
OVERRIDE_YAML = "override.yaml"

# directories for heron sandbox
SANDBOX_CONF_DIR = "./heron-conf"

# config file for heron cli
CLIENT_YAML = "client.yaml"

# cli configs for role and env
ROLE_REQUIRED = "heron.config.role.required"
ENV_REQUIRED = "heron.config.env.required"


################################################################################
# Create a tar file with a given set of files
################################################################################
def create_tar(tar_filename, files, config_dir, config_files):
    '''
    :param tar_filename:
    :param files:
    :param config_dir:
    :param config_files:
    :return:
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


################################################################################
# Retrieve the given subparser from parser
################################################################################
def get_subparser(parser, command):
    '''
    :param parser:
    :param command:
    :return:
    '''
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
def identity(xvar):
    '''
    :param x:
    :return:
    '''
    return xvar


def cygpath(xvar):
    '''
    :param x:
    :return:
    '''
    command = ['cygpath', '-wp', xvar]
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, _ = process.communicate()
    lines = output.split("\n")
    return lines[0]


def normalized_class_path(xvar):
    '''
    :param x:
    :return:
    '''
    if sys.platform == 'cygwin':
        return cygpath(xvar)
    return identity(xvar)


################################################################################
# Get the normalized class path of all jars
################################################################################
def get_classpath(jars):
    '''
    :param jars:
    :return:
    '''
    return ':'.join([normalized_class_path(jar) for jar in jars])

################################################################################
# Get the root of heron dir and various sub directories depending on platform
################################################################################
def get_heron_dir():
    """
    This will extract heron directory from .pex file.
    :return: root location for heron-cli.
    """
    path = "/".join(os.path.realpath(__file__).split('/')[:-7])
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


################################################################################
# Get the sandbox directories and config files
################################################################################
def get_heron_sandbox_conf_dir():
    """
    This will provide heron conf directory in the sandbox
    :return: relative path of heron sandbox conf directory
    """
    return SANDBOX_CONF_DIR


################################################################################
# Get all the heron lib jars with the absolute paths
################################################################################
def get_heron_libs(local_jars):
    '''
    :param local_jars:
    :return:
    '''
    heron_lib_dir = get_heron_lib_dir()
    heron_libs = [os.path.join(heron_lib_dir, f) for f in local_jars]
    return heron_libs


################################################################################
# Get the cluster to which topology is submitted
################################################################################
def get_heron_cluster(cluster_role_env):
    '''
    :param cluster_role_env:
    :return:
    '''
    return cluster_role_env.split('/')[0]


################################################################################
# Parse cluster/[role]/[environ], supply default, if not provided, not required
################################################################################
def parse_cluster_role_env(cluster_role_env, config_path):
    '''
    :param cluster_role_env:
    :param config_path:
    :return:
    '''
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
                    if (ROLE_REQUIRED in cli_confs) and (cli_confs[ROLE_REQUIRED]):
                        raise Exception("role required but not provided")
                    else:
                        parts.append(getpass.getuser())

                # if environ is required but not provided, raise exception
                if len(parts) == 2:
                    if (ENV_REQUIRED in cli_confs) and (cli_confs[ENV_REQUIRED]):
                        raise Exception("environ required but not provided")
                    else:
                        parts.append(ENVIRON)

    # if cluster or role or environ is empty, print
    if len(parts[0]) == 0 or len(parts[1]) == 0 or len(parts[2]) == 0:
        # FIXME: this looks like a legitimate bug.  what variables are supposed to be here?
        print "Failed to parse %s: %s" % (argstr, namespace[argstr])
        sys.exit(1)

    return (parts[0], parts[1], parts[2])


################################################################################
# Parse the command line for overriding the defaults
################################################################################
def parse_override_config(namespace):
    '''
    :param namespace:
    :return:
    '''
    try:
        tmp_dir = tempfile.mkdtemp()
        override_config_file = os.path.join(tmp_dir, OVERRIDE_YAML)
        with open(override_config_file, 'w') as handle:
            for config in namespace:
                handle.write("%s\n" % config.replace('=', ': '))

        return override_config_file
    except Exception as ex:
        raise Exception("Failed to parse override config: %s" % str(ex))


################################################################################
# Get the path of java executable
################################################################################
def get_java_path():
    '''
    :return:
    '''
    java_home = os.environ.get("JAVA_HOME")
    return os.path.join(java_home, BIN_DIR, "java")


################################################################################
# Check if the java home set
################################################################################
def check_java_home_set():
    '''
    :return:
    '''
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


################################################################################
# Check if the release.yaml file exists
################################################################################
def check_release_file_exists():
    '''
    :return:
    '''
    release_file = get_heron_release_file()

    # if the file does not exist and is not a file
    if not os.path.isfile(release_file):
        Log.error("%s file not found: %s" % release_file)
        return False

    return True
