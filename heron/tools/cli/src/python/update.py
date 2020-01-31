#!/usr/bin/env python
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

''' update.py '''
import argparse
import re

from heron.common.src.python.utils.log import Log
from heron.tools.cli.src.python.result import SimpleResult, Status

import heron.tools.cli.src.python.args as args
import heron.tools.cli.src.python.cli_helper as cli_helper
import heron.tools.cli.src.python.jars as jars
import heron.tools.common.src.python.utils.config as config

def create_parser(subparsers):
  """ Create the parse for the update command """
  parser = subparsers.add_parser(
      'update',
      help='Update a topology',
      usage="%(prog)s [options] cluster/[role]/[env] <topology-name> "
      + "[--component-parallelism <name:value>] "
      + "[--container-number value] "
      + "[--runtime-config [component:]<name:value>]",
      add_help=True)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  args.add_config(parser)
  args.add_dry_run(parser)
  args.add_service_url(parser)
  args.add_verbose(parser)

  # Special parameters for update command
  def parallelism_type(value):
    pattern = re.compile(r"^[\w\.-]+:[\d]+$")
    if not pattern.match(value):
      raise argparse.ArgumentTypeError(
          "Invalid syntax for component parallelism (<component_name:value>): %s" % value)
    return value

  parser.add_argument(
      '--component-parallelism',
      action='append',
      type=parallelism_type,
      required=False,
      help='Component name and the new parallelism value '
      + 'colon-delimited: <component_name>:<parallelism>')

  def runtime_config_type(value):
    pattern = re.compile(r"^([\w\.-]+:){1,2}[\w\.-]+$")
    if not pattern.match(value):
      raise argparse.ArgumentTypeError(
          "Invalid syntax for runtime config ([component:]<name:value>): %s"
          % value)
    return value

  parser.add_argument(
      '--runtime-config',
      action='append',
      type=runtime_config_type,
      required=False,
      help='Runtime configurations for topology and components '
      + 'colon-delimited: [component:]<name>:<value>')

  def container_number_type(value):
    pattern = re.compile(r"^\d+$")
    if not pattern.match(value):
      raise argparse.ArgumentTypeError(
          "Invalid syntax for container number (value): %s"
          % value)
    return value

  parser.add_argument(
      '--container-number',
      action='append',
      type=container_number_type,
      required=False,
      help='Number of containers <value>')

  parser.set_defaults(subcommand='update')
  return parser

def build_extra_args_dict(cl_args):
  """ Build extra args map """
  # Check parameters
  component_parallelism = cl_args['component_parallelism']
  runtime_configs = cl_args['runtime_config']
  container_number = cl_args['container_number']
  # Users need to provide either (component-parallelism || container_number) or runtime-config
  if (component_parallelism and runtime_configs) or (container_number and runtime_configs):
    raise Exception(
        "(component-parallelism or container_num) and runtime-config " +
        "can't be updated at the same time")

  dict_extra_args = {}

  nothing_set = True
  if component_parallelism:
    dict_extra_args.update({'component_parallelism': component_parallelism})
    nothing_set = False

  if container_number:
    dict_extra_args.update({'container_number': container_number})
    nothing_set = False

  if runtime_configs:
    dict_extra_args.update({'runtime_config': runtime_configs})
    nothing_set = False

  if nothing_set:
    raise Exception(
        "Missing arguments --component-parallelism or --runtime-config or --container-number")

  if cl_args['dry_run']:
    dict_extra_args.update({'dry_run': True})
    if 'dry_run_format' in cl_args:
      dict_extra_args.update({'dry_run_format': cl_args["dry_run_format"]})

  return dict_extra_args


def convert_args_dict_to_list(dict_extra_args):
  """ flatten extra args """
  list_extra_args = []
  if 'component_parallelism' in dict_extra_args:
    list_extra_args += ["--component_parallelism",
                        ','.join(dict_extra_args['component_parallelism'])]
  if 'runtime_config' in dict_extra_args:
    list_extra_args += ["--runtime_config",
                        ','.join(dict_extra_args['runtime_config'])]
  if 'container_number' in dict_extra_args:
    list_extra_args += ["--container_number",
                        ','.join(dict_extra_args['container_number'])]
  if 'dry_run' in dict_extra_args and dict_extra_args['dry_run']:
    list_extra_args += ['--dry_run']
  if 'dry_run_format' in dict_extra_args:
    list_extra_args += ['--dry_run_format', dict_extra_args['dry_run_format']]

  return list_extra_args

# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  """ run the update command """

  Log.debug("Update Args: %s", cl_args)

  # Build jar list
  extra_lib_jars = jars.packing_jars()
  action = "update topology%s" % (' in dry-run mode' if cl_args["dry_run"] else '')

  # Build extra args
  dict_extra_args = {}
  try:
    dict_extra_args = build_extra_args_dict(cl_args)
  except Exception as err:
    return SimpleResult(Status.InvocationError, str(err))
    # return SimpleResult(Status.InvocationError, err.message)

  # Execute
  if cl_args['deploy_mode'] == config.SERVER_MODE:
    return cli_helper.run_server(command, cl_args, action, dict_extra_args)
  else:
    # Convert extra argument to commandline format and then execute
    list_extra_args = convert_args_dict_to_list(dict_extra_args)
    return cli_helper.run_direct(command, cl_args, action, list_extra_args, extra_lib_jars)
