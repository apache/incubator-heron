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
from heron.common.src.python.color import Log

import heron.cli.src.python.args as args
import heron.cli.src.python.execute as execute
import heron.cli.src.python.jars as jars
import heron.common.src.python.utils as utils


def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'restart',
      help='Restart a topology',
      usage="%(prog)s [options] cluster/[role]/[env] topology-name [container-id]",
      add_help=False)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  parser.add_argument(
      'container-id',
      nargs='?',
      type=int,
      default=-1,
      help='Identifier of the container to be restarted')

  args.add_config(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='restart')
  return parser


# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  '''
  :param command:
  :param parser:
  :param cl_args:
  :param unknown_args:
  :return:
  '''
  try:
    topology_name = cl_args['topology-name']
    container_id = cl_args['container-id']

    new_args = [
        "--cluster", cl_args['cluster'],
        "--role", cl_args['role'],
        "--environment", cl_args['environ'],
        "--heron_home", utils.get_heron_dir(),
        "--config_path", cl_args['config_path'],
        "--override_config_file", cl_args['override_config_file'],
        "--release_file", utils.get_heron_release_file(),
        "--topology_name", topology_name,
        "--command", command,
        "--container_id", str(container_id)
    ]

    lib_jars = utils.get_heron_libs(jars.scheduler_jars() + jars.statemgr_jars())

    # invoke the runtime manager to kill the topology
    execute.heron_class(
        'com.twitter.heron.scheduler.RuntimeManagerMain',
        lib_jars,
        extra_jars=[],
        args=new_args
    )

  except Exception as ex:
    print 'Error: %s' % str(ex)
    Log.error('Failed to restart topology \'%s\'' % topology_name)
    return False

  Log.info('Successfully restarted topology \'%s\'' % topology_name)
  return True
