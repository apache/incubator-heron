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

''' cli_helper.py '''
import logging
import requests
import heron.tools.common.src.python.utils.config as config
from heron.tools.cli.src.python.result import SimpleResult, Status
import heron.tools.cli.src.python.args as args
import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.cli.src.python.rest as rest

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
      add_help=True)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  args.add_config(parser)
  args.add_service_url(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand=action)
  return parser

################################################################################
def flatten_args(fargs):
  temp_args = []
  for k, v in list(fargs.items()):
    if isinstance(v, list):
      temp_args.extend([(k, value) for value in v])
    else:
      temp_args.append((k, v))
  return temp_args

################################################################################
# pylint: disable=dangerous-default-value
def run_server(command, cl_args, action, extra_args=dict()):
  '''
  helper function to take action on topologies using REST API
  :param command:
  :param cl_args:
  :param action:        description of action taken
  :return:
  '''
  topology_name = cl_args['topology-name']

  service_endpoint = cl_args['service_url']
  apiroute = rest.ROUTE_SIGNATURES[command][1] % (
      cl_args['cluster'],
      cl_args['role'],
      cl_args['environ'],
      topology_name
  )
  service_apiurl = service_endpoint + apiroute
  service_method = rest.ROUTE_SIGNATURES[command][0]

  # convert the dictionary to a list of tuples
  data = flatten_args(extra_args)

  err_msg = "Failed to %s: %s" % (action, topology_name)
  succ_msg = "Successfully %s: %s" % (action, topology_name)

  try:
    r = service_method(service_apiurl, data=data)
    s = Status.Ok if r.status_code == requests.codes.ok else Status.HeronError
    if r.status_code != requests.codes.ok:
      Log.error(r.json().get('message', "Unknown error from API server %d" % r.status_code))
  except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as err:
    Log.error(err)
    return SimpleResult(Status.HeronError, err_msg, succ_msg)

  return SimpleResult(s, err_msg, succ_msg)

################################################################################
# pylint: disable=dangerous-default-value
def run_direct(command, cl_args, action, extra_args=[], extra_lib_jars=[]):
  '''
  helper function to take action on topologies
  :param command:
  :param cl_args:
  :param action:        description of action taken
  :return:
  '''
  topology_name = cl_args['topology-name']

  new_args = [
      "--cluster", cl_args['cluster'],
      "--role", cl_args['role'],
      "--environment", cl_args['environ'],
      "--submit_user", cl_args['submit_user'],
      "--heron_home", config.get_heron_dir(),
      "--config_path", cl_args['config_path'],
      "--override_config_file", cl_args['override_config_file'],
      "--release_file", config.get_heron_release_file(),
      "--topology_name", topology_name,
      "--command", command,
  ]
  new_args += extra_args
  lib_jars = config.get_heron_libs(jars.scheduler_jars() + jars.statemgr_jars())
  lib_jars += extra_lib_jars

  if Log.getEffectiveLevel() == logging.DEBUG:
    new_args.append("--verbose")

  # invoke the runtime manager to kill the topology
  result = execute.heron_class(
      'org.apache.heron.scheduler.RuntimeManagerMain',
      lib_jars,
      extra_jars=[],
      args=new_args
  )

  err_msg = "Failed to %s: %s" % (action, topology_name)
  succ_msg = "Successfully %s: %s" % (action, topology_name)
  result.add_context(err_msg, succ_msg)
  return result

################################################################################
def run(command, cl_args, action, extra_lib_jars=[]):
  if cl_args['deploy_mode'] == config.SERVER_MODE:
    return run_server(command, cl_args, action, extra_args=dict())
  else:
    return run_direct(command, cl_args, action, extra_args=[], extra_lib_jars=extra_lib_jars)
