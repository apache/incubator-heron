#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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
import heron.tools.cli.src.python.cli_helper as cli_helper
import heron.tools.cli.src.python.jars as jars
import heron.tools.common.src.python.utils.config as config

import argparse
import re

def create_parser(subparsers):
  """ Create the parse for the update command """
  parser = subparsers.add_parser(
      'update',
      help='Update a topology',
      usage="%(prog)s [options] cluster/[role]/[env] <topology-name> "
      + "[--component-parallelism <name:value>] [--user-config <topology|component-name:config=value>]",
      add_help=True)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  def parallelism_type(value):
    pattern = re.compile(r"^[\w\.-]+:[\d]+$")
    if not pattern.match(value):
      raise argparse.ArgumentTypeError(
          'Invalid syntax for component parallelism (<component_name>:<value>): %s' % value)
    return value

  parser.add_argument(
      '--component-parallelism',
      action='append',
      type=parallelism_type,
      required=False,
      help='Component name and the new parallelism value '
      + 'colon-delimited: [component_name]:[parallelism]')

  def user_config_type(value):
    pattern = re.compile(r"^[\w\.-]+:[\w\.-_]+=[\w\.-_]+$")
    if not pattern.match(value):
      raise argparse.ArgumentTypeError(
          'Invalid syntax for user config (<topology|component_name>:<config>=<value>): %s'
              % value)
    return value

  parser.add_argument(
      '--user-config',
      action='append',
      type=user_config_type,
      required=False,
      help='Runtime config topology and/or components'
      + 'colon-delimited: [topology|component_name]:[config]=[value]')

  args.add_config(parser)
  args.add_dry_run(parser)
  args.add_service_url(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='update')
  return parser


# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  """ run the update command """

  Log.debug("Update Args: %s", cl_args)

  # Check parameters
  component_parallelisms = cl_args['component_parallelism']
  user_configs = cl_args['user_config']
  # Users need to provide at least one component-parallelism or at least one user-config
  if len(component_parallelisms) > 0 and len(user_configs) > 0:
    err_context = "--component-parallelism and --user-config can not be updated at the same time"
      return SimpleResult(Status.InvocationError, err_context)

  # Build and run command
  extra_lib_jars = jars.packing_jars()
  action = "update topology%s" % (' in dry-run mode' if cl_args["dry_run"] else '')

  if cl_args['deploy_mode'] == config.SERVER_MODE:
    # Build extra args
    dict_extra_args = {}
    if len(component_parallelisms) > 0:
        dict_extra_args.update({"component_parallelism", component_parallelisms})
    elif len(user_configs) > 0:
        dict_extra_args.update({"user_config", user_configs})
    else:
      err_context = "Missing arguments --component-parallelism or --user-config"
      return SimpleResult(Status.InvocationError, err_context)

    if cl_args["dry_run"]:
      dict_extra_args.update({'dry_run': True})
      if "dry_run_format" in cl_args:
        dict_extra_args.update({"dry_run_format", cl_args["dry_run_format"]})

    return cli_helper.run_server(command, cl_args, action, dict_extra_args)
  else:
    # Build extra args
    list_extra_args = []
    if len(component_parallelisms) > 0:
      list_extra_args += ["--component_parallelism", ','.join(component_parallelisms)]
    elif len(user_configs) > 0:
      list_extra_args += ["--user_config", ','.join(user_configs)]
    else:
      err_context = "Missing arguments --component-parallelism or --user-config"
      return SimpleResult(Status.InvocationError, err_context)

    if cl_args["dry_run"]:
      list_extra_args.append('--dry_run')
      if "dry_run_format" in cl_args:
        list_extra_args += ["--dry_run_format", cl_args["dry_run_format"]]

    return cli_helper.run_direct(command, cl_args, action, list_extra_args, extra_lib_jars)
