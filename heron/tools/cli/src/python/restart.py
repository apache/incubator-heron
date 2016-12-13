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
import heron.tools.cli.src.python.args as args
import heron.tools.cli.src.python.cli_helper as cli_helper


def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'restart',
      help='Restart a topology',
      usage="%(prog)s [options] cluster/[role]/[env] <topology-name> [container-id]",
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
  container_id = cl_args['container-id']
  extra_args = ["--container_id", str(container_id)]

  return cli_helper.run(command, cl_args, "restart topology", extra_args=extra_args)
