#!/usr/bin/env python3
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

''' restart.py '''
from heron.common.src.python.utils.log import Log
from heron.tools.cli.src.python import args
from heron.tools.cli.src.python import cli_helper
from heron.tools.common.src.python.utils import config

def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'restart',
      help='Restart a topology',
      usage="%(prog)s [options] cluster/[role]/[env] <topology-name> [container-id]",
      add_help=True)

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
  args.add_service_url(parser)
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
  Log.debug("Restart Args: %s", cl_args)
  container_id = cl_args['container-id']

  message = "restart topology"
  if container_id >= 0:
    message = "restart container " + str(container_id) + " of topology"

  if cl_args['deploy_mode'] == config.SERVER_MODE:
    dict_extra_args = {"container_id": str(container_id)}
    return cli_helper.run_server(command, cl_args, message, extra_args=dict_extra_args)
  list_extra_args = ["--container_id", str(container_id)]
  return cli_helper.run_direct(command, cl_args, message, extra_args=list_extra_args)
