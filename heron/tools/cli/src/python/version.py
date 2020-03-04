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

''' version.py '''
from heron.common.src.python.utils.log import Log
from heron.tools.cli.src.python.result import SimpleResult, Status
import heron.tools.cli.src.python.args as cli_args
import heron.tools.common.src.python.utils.config as config
import heron.tools.cli.src.python.cdefs as cdefs
import heron.tools.cli.src.python.rest as rest

import sys
import requests

def add_version_titles(parser):
  '''
  :param parser:
  :return:
  '''
  # pylint: disable=protected-access
  parser._positionals.title = "Optional positional arguments"
  parser._optionals.title = "Optional arguments"
  return parser

def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'version',
      help='Print version of heron-cli',
      usage="%(prog)s [options] [cluster]",
      add_help=True)

  add_version_titles(parser)

  parser.add_argument(
      'cluster',
      nargs='?',
      type=str,
      default="",
      help='Name of the cluster')

  cli_args.add_service_url(parser)

  parser.set_defaults(subcommand='version')
  return parser

# pylint: disable=unused-argument,superfluous-parens
def run(command, parser, cl_args, unknown_args):
  '''
  :param command:
  :param parser:
  :param args:
  :param unknown_args:
  :return:
  '''
  cluster = cl_args['cluster']

  # server mode
  if cluster:
    config_file = config.heron_rc_file()
    client_confs = dict()

    # Read the cluster definition, if not found
    client_confs = cdefs.read_server_mode_cluster_definition(cluster, cl_args, config_file)

    if not client_confs[cluster]:
      Log.error('Neither service url nor %s cluster definition in %s file', cluster, config_file)
      return SimpleResult(Status.HeronError)

    # if cluster definition exists, but service_url is not set, it is an error
    if not 'service_url' in client_confs[cluster]:
      Log.error('No service url for %s cluster in %s', cluster, config_file)
      sys.exit(1)

    service_endpoint = cl_args['service_url']
    service_apiurl = service_endpoint + rest.ROUTE_SIGNATURES[command][1]
    service_method = rest.ROUTE_SIGNATURES[command][0]

    try:
      r = service_method(service_apiurl)
      if r.status_code != requests.codes.ok:
        Log.error(r.json().get('message', "Unknown error from API server %d" % r.status_code))
      sorted_items = sorted(list(r.json().items()), key=lambda tup: tup[0])
      for key, value in sorted_items:
        print("%s : %s" % (key, value))
    except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as err:
      Log.error(err)
      return SimpleResult(Status.HeronError)
  else:
    config.print_build_info()

  return SimpleResult(Status.Ok)
