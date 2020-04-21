#!/usr/bin/env python2.7
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


''' main.py '''
import argparse
import atexit
import getpass
import os
import shutil
import sys
import time
import traceback

import heron.common.src.python.utils.log as log
import heron.tools.common.src.python.utils.config as config
import heron.tools.cli.src.python.cdefs as cdefs
import heron.tools.cli.src.python.cliconfig as cliconfig
import heron.tools.cli.src.python.help as cli_help
import heron.tools.cli.src.python.activate as activate
import heron.tools.cli.src.python.deactivate as deactivate
import heron.tools.cli.src.python.kill as kill
import heron.tools.cli.src.python.result as result
import heron.tools.cli.src.python.restart as restart
import heron.tools.cli.src.python.submit as submit
import heron.tools.cli.src.python.update as update
import heron.tools.cli.src.python.version as version
import heron.tools.cli.src.python.config as hconfig

from heron.tools.cli.src.python.opts import cleaned_up_files

Log = log.Log

HELP_EPILOG = '''Getting more help:
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''


# pylint: disable=protected-access,superfluous-parens
class _HelpAction(argparse._HelpAction):
  def __call__(self, parser, namespace, values, option_string=None):
    parser.print_help()

    # retrieve subparsers from parser
    subparsers_actions = [
        action for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    ]

    # there will probably only be one subparser_action,
    # but better save than sorry
    for subparsers_action in subparsers_actions:
      # get all subparsers and print help
      for choice, subparser in list(subparsers_action.choices.items()):
        print("Subparser '{}'".format(choice))
        print(subparser.format_help())
        return

################################################################################
def get_command_handlers():
  '''
  Create a map of command names and handlers
  '''
  return {
      'activate': activate,
      'config': hconfig,
      'deactivate': deactivate,
      'help': cli_help,
      'kill': kill,
      'restart': restart,
      'submit': submit,
      'update': update,
      'version': version
  }

################################################################################
def create_parser(command_handlers):
  '''
  Main parser
  :return:
  '''
  parser = argparse.ArgumentParser(
      prog='heron',
      epilog=HELP_EPILOG,
      formatter_class=config.SubcommandHelpFormatter,
      add_help=True)

  subparsers = parser.add_subparsers(
      title="Available commands",
      metavar='<command> <options>')

  command_list = sorted(command_handlers.items())
  for command in command_list:
    command[1].create_parser(subparsers)

  return parser


################################################################################
def run(handlers, command, parser, command_args, unknown_args):
  '''
  Run the command
  :param command:
  :param parser:
  :param command_args:
  :param unknown_args:
  :return:
  '''

  if command in handlers:
    return handlers[command].run(command, parser, command_args, unknown_args)
  else:
    err_context = 'Unknown subcommand: %s' % command
    return result.SimpleResult(result.Status.InvocationError, err_context)

def cleanup(files):
  '''
  :param files:
  :return:
  '''
  for cur_file in files:
    if os.path.isdir(cur_file):
      shutil.rmtree(cur_file)
    else:
      shutil.rmtree(os.path.dirname(cur_file))


################################################################################
def check_environment():
  '''
  Check whether the environment variables are set
  :return:
  '''
  if not config.check_java_home_set():
    sys.exit(1)

  if not config.check_release_file_exists():
    sys.exit(1)

################################################################################
# pylint: disable=unused-argument
def server_deployment_mode(command, parser, cluster, cl_args):
  '''
  check the server deployment mode for the given cluster
  if it is valid return the valid set of args
  :param cluster:
  :param cl_args:
  :return:
  '''
  # Read the cluster definition, if not found
  client_confs = cdefs.read_server_mode_cluster_definition(cluster, cl_args)

  if not client_confs[cluster]:
    return dict()

  # tell the user which definition that we are using
  if not cl_args.get('service_url', None):
    Log.debug("Using cluster definition from file %s" \
        % cliconfig.get_cluster_config_file(cluster))
  else:
    Log.debug("Using cluster service url %s" % cl_args['service_url'])

  # if cluster definition exists, but service_url is not set, it is an error
  if not 'service_url' in client_confs[cluster]:
    config_file = cliconfig.get_cluster_config_file(cluster)
    Log.error('No service url for %s cluster in %s', cluster, config_file)
    sys.exit(1)

  # get overrides
  if 'config_property' in cl_args:
    pass

  try:
    cluster_role_env = (cl_args['cluster'], cl_args['role'], cl_args['environ'])
    config.server_mode_cluster_role_env(cluster_role_env, client_confs)
    cluster_tuple = config.defaults_cluster_role_env(cluster_role_env)
  except Exception as ex:
    Log.error("Argument cluster/[role]/[env] is not correct: %s", str(ex))
    sys.exit(1)

  new_cl_args = dict()
  new_cl_args['cluster'] = cluster_tuple[0]
  new_cl_args['role'] = cluster_tuple[1]
  new_cl_args['environ'] = cluster_tuple[2]
  new_cl_args['service_url'] = client_confs[cluster]['service_url'].rstrip('/')
  new_cl_args['deploy_mode'] = config.SERVER_MODE

  cl_args.update(new_cl_args)
  return cl_args

################################################################################
# pylint: disable=superfluous-parens
def direct_deployment_mode(command, parser, cluster, cl_args):
  '''
  check the direct deployment mode for the given cluster
  if it is valid return the valid set of args
  :param command:
  :param parser:
  :param cluster:
  :param cl_args:
  :return:
  '''

  cluster = cl_args['cluster']
  try:
    config_path = cl_args['config_path']
    override_config_file = config.parse_override_config_and_write_file(cl_args['config_property'])
  except KeyError:
    # if some of the arguments are not found, print error and exit
    subparser = config.get_subparser(parser, command)
    print(subparser.format_help())
    return dict()

  # check if the cluster config directory exists
  if not cdefs.check_direct_mode_cluster_definition(cluster, config_path):
    Log.error("Cluster config directory \'%s\' does not exist", config_path)
    return dict()

  config_path = config.get_heron_cluster_conf_dir(cluster, config_path)
  if not os.path.isdir(config_path):
    Log.error("Cluster config directory \'%s\' does not exist", config_path)
    return dict()

  Log.info("Using cluster definition in %s" % config_path)

  try:
    cluster_role_env = (cl_args['cluster'], cl_args['role'], cl_args['environ'])
    config.direct_mode_cluster_role_env(cluster_role_env, config_path)
    cluster_tuple = config.defaults_cluster_role_env(cluster_role_env)
  except Exception as ex:
    Log.error("Argument cluster/[role]/[env] is not correct: %s", str(ex))
    return dict()

  new_cl_args = dict()
  new_cl_args['cluster'] = cluster_tuple[0]
  new_cl_args['role'] = cluster_tuple[1]
  new_cl_args['environ'] = cluster_tuple[2]
  new_cl_args['config_path'] = config_path
  new_cl_args['override_config_file'] = override_config_file
  new_cl_args['deploy_mode'] = config.DIRECT_MODE

  cl_args.update(new_cl_args)
  return cl_args

################################################################################
def deployment_mode(command, parser, cl_args):
  # first check if it is server mode
  new_cl_args = server_deployment_mode(command, parser, cl_args['cluster'], cl_args)
  if len(new_cl_args) > 0:
    return new_cl_args

  # now check if it is direct mode
  new_cl_args = direct_deployment_mode(command, parser, cl_args['cluster'], cl_args)
  if len(new_cl_args) > 0:
    return new_cl_args

  return dict()


################################################################################
def extract_common_args(command, parser, cl_args):
  '''
  Extract all the common args for all commands
  :param command:
  :param parser:
  :param cl_args:
  :return:
  '''
  try:
    cluster_role_env = cl_args.pop('cluster/[role]/[env]')
  except KeyError:
    try:
      cluster_role_env = cl_args.pop('cluster')  # for version command
    except KeyError:
      # if some of the arguments are not found, print error and exit
      subparser = config.get_subparser(parser, command)
      print(subparser.format_help())
      return dict()

  new_cl_args = dict()
  cluster_tuple = config.get_cluster_role_env(cluster_role_env)
  new_cl_args['cluster'] = cluster_tuple[0]
  new_cl_args['role'] = cluster_tuple[1]
  new_cl_args['environ'] = cluster_tuple[2]
  new_cl_args['submit_user'] = getpass.getuser()

  cl_args.update(new_cl_args)
  return cl_args

################################################################################
def execute(handlers, local_commands):
  '''
  Run the command
  :return:
  '''
  # verify if the environment variables are correctly set
  check_environment()

  # create the argument parser
  parser = create_parser(handlers)

  # if no argument is provided, print help and exit
  if len(sys.argv[1:]) == 0:
    parser.print_help()
    return 0

  # insert the boolean values for some of the options
  sys.argv = config.insert_bool_values(sys.argv)

  try:
    # parse the args
    args, unknown_args = parser.parse_known_args()
  except ValueError as ex:
    Log.error("Error while parsing arguments: %s", str(ex))
    Log.debug(traceback.format_exc())
    sys.exit(1)

  command_line_args = vars(args)

  # set log level
  log.set_logging_level(command_line_args)
  Log.debug("Input Command Line Args: %s", command_line_args)

# command to be execute
  command = command_line_args['subcommand']
  is_local_command = command in local_commands

  if command == 'version':
    results = run(handlers, command, parser, command_line_args, unknown_args)
    return 0 if result.is_successful(results) else 1

  if not is_local_command:
    log.set_logging_level(command_line_args)
    Log.debug("Input Command Line Args: %s", command_line_args)

    # determine the mode of deployment
    command_line_args = extract_common_args(command, parser, command_line_args)
    command_line_args = deployment_mode(command, parser, command_line_args)

    # bail out if args are empty
    if not command_line_args:
      return 1

    # register dirs cleanup function during exit
    if command_line_args['deploy_mode'] == config.DIRECT_MODE and command != "version":
      cleaned_up_files.append(command_line_args['override_config_file'])
      atexit.register(cleanup, cleaned_up_files)

  # print the input parameters, if verbose is enabled
  Log.debug("Processed Command Line Args: %s", command_line_args)

  start = time.time()
  results = run(handlers, command, parser, command_line_args, unknown_args)
  if not is_local_command:
    result.render(results)
  end = time.time()

  if not is_local_command:
    sys.stdout.flush()
    Log.debug('Elapsed time: %.3fs.', (end - start))

  return 0 if result.is_successful(results) else 1

def main():
  # Create a map of supported commands and handlers
  command_handlers = get_command_handlers()
  # Execute
  local_commands = ('help', 'version', 'config')
  return execute(command_handlers, local_commands)


if __name__ == "__main__":
  sys.exit(main())
