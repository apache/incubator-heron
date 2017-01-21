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
import heron.tools.cli.src.python.jars as jars

import argparse
import re

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
  args.add_dry_run(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='update')
  return parser


# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  """ run the update command """
  extra_args = ["--component_parallelism", ','.join(cl_args['component_parallelism'])]
  extra_lib_jars = jars.packing_jars()
  if cl_args["dry_run"]:
    extra_args.append('--dry_run')
  if "dry_run_format" in cl_args:
    extra_args += ["--dry_run_format", cl_args["dry_run_format"]]

  return cli_helper.run(command, cl_args, "update topology", extra_args, extra_lib_jars)
