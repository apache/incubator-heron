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
''' cli_helper.py '''
import logging
import heron.tools.common.src.python.utils.config as config
import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.cli.src.python.args as args

from heron.common.src.python.utils.log import Log

################################################################################
def create_parser(subparsers, action, help_arg):
  '''
  :param subparsers:
  :param action:
  :param help_arg:
  :return:
  '''
  parser = subparsers.add_parser(
      action,
      help=help_arg,
      usage="%(prog)s [options] cluster/[role]/[env] <topology-name>",
      add_help=False)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  args.add_config(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand=action)
  return parser


################################################################################
# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args, action):
  '''
  helper function to take action on topologies
  :param command:
  :param parser:
  :param cl_args:
  :param unknown_args:
  :param action:        description of action taken
  :return:
  '''
  topology_name = cl_args['topology-name']

  new_args = [
      "--cluster", cl_args['cluster'],
      "--role", cl_args['role'],
      "--environment", cl_args['environ'],
      "--heron_home", config.get_heron_dir(),
      "--config_path", cl_args['config_path'],
      "--override_config_file", cl_args['override_config_file'],
      "--release_file", config.get_heron_release_file(),
      "--topology_name", topology_name,
      "--command", command,
  ]

  if Log.getEffectiveLevel() == logging.DEBUG:
    new_args.append("--verbose")

  lib_jars = config.get_heron_libs(jars.scheduler_jars() + jars.statemgr_jars())

  # invoke the runtime manager to kill the topology
  _, _, retcode = execute.heron_class(
      'com.twitter.heron.scheduler.RuntimeManagerMain',
      lib_jars,
      extra_jars=[],
      args=new_args
  )

  if retcode != 0:
    Log.error('Failed to %s \'%s\'', action, topology_name)
    return False
  else:
    Log.info('Successfully executed %s \'%s\'', action, topology_name)
    return True
