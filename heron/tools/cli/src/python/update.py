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
''' restart.py '''
from heron.common.src.python.utils.log import Log
import heron.tools.cli.src.python.args as args
import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.common.src.python.utils.config as config

import argparse
import logging
import re
import traceback

def create_parser(subparsers):
  """ Create the parse for the update command """
  parser = subparsers.add_parser(
      'update',
      help='Update a topology',
      usage="%(prog)s [options] cluster/[role]/[env] <topology-name> "
      + "--component-parallelism <name:value>",
      add_help=False)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  def parallelism_type(value):
    pattern = re.compile(r"^[\w-]+:[\d]+$")
    if not pattern.match(value):
      raise argparse.ArgumentTypeError(
          'Invalid syntax for component parallelism (<component_name>:<value>): %s' % value)
    return value

  parser.add_argument(
      '--component-parallelism',
      action='append',
      type=parallelism_type,
      required=True,
      help='Component name and the new parallelism value '
      + 'colon-delimited: [component_name]:[parallelism]')

  args.add_config(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='update')
  return parser


# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  """ run the update command """
  topology_name = cl_args['topology-name']
  try:
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
        "--component_parallelism", ','.join(cl_args['component_parallelism']),
    ]

    if Log.getEffectiveLevel() == logging.DEBUG:
      new_args.append("--verbose")

    lib_jars = config.get_heron_libs(
        jars.scheduler_jars() + jars.statemgr_jars() + jars.packing_jars()
    )

    # invoke the runtime manager to kill the topology
    execute.heron_class(
        'com.twitter.heron.scheduler.RuntimeManagerMain',
        lib_jars,
        extra_jars=[],
        args=new_args
    )

  except Exception as ex:
    Log.error('Failed to update topology \'%s\': %s', topology_name, traceback.format_exc(ex))
    return False

  Log.info('Successfully updated topology \'%s\'' % topology_name)
  return True
