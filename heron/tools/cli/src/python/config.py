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

''' config.py '''
import heron.tools.cli.src.python.cliconfig as cliconfig
from heron.tools.cli.src.python.result import SimpleResult, Status


def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'config',
      help='Config properties for a cluster',
      usage="%(prog)s [cluster]",
      add_help=True)

  parser.add_argument(
      'cluster',
      help='Cluster to configure'
  )

  ex_subparsers = parser.add_subparsers(
      title="Commands",
      description=None)

  # add config list parser
  list_parser = ex_subparsers.add_parser(
      'list',
      help='List config properties for a cluster',
      usage="%(prog)s",
      add_help=True)
  list_parser.set_defaults(configcommand='list')

  # add config set parser
  set_parser = ex_subparsers.add_parser(
      'set',
      help='Set a cluster config property',
      usage="%(prog)s [property] [value]",
      add_help=True)

  set_parser.add_argument(
      'property',
      help='Config property to set'
  )

  set_parser.add_argument(
      'value',
      help='Value of config property'
  )
  set_parser.set_defaults(configcommand='set')

  # add config unset parser
  unset_parser = ex_subparsers.add_parser(
      'unset',
      help='Unset a cluster config property',
      usage="%(prog)s [property]",
      add_help=True)

  unset_parser.add_argument(
      'property',
      help='Config property to unset'
  )
  unset_parser.set_defaults(configcommand='unset')

  parser.set_defaults(subcommand='config')
  return parser


# pylint: disable=superfluous-parens
def _list(cl_args):
  cluster = cl_args['cluster']
  config = cliconfig.cluster_config(cluster)
  if config:
    for k, v in list(config.items()):
      print("%s = %s" % (str(k), str(v)))
  else:
    print("No config for cluster %s" % cluster)

  return SimpleResult(Status.Ok)

# pylint: disable=superfluous-parens
def _set(cl_args):
  cluster, prop, value = cl_args['cluster'], cl_args['property'], cl_args['value']
  if cliconfig.is_valid_property(prop):
    cliconfig.set_property(cluster, prop, value)
    print("Updated property [%s] for cluster %s" % (prop, cluster))
  else:
    print("Error: Unknown property [%s] for cluster %s" % (prop, cluster))

  return SimpleResult(Status.Ok)


# pylint: disable=superfluous-parens
def _unset(cl_args):
  cluster, prop = cl_args['cluster'], cl_args['property']
  if cliconfig.is_valid_property(prop):
    cliconfig.unset_property(cluster, prop)
    print("Cleared property [%s] for cluster %s" % (prop, cluster))
  else:
    print("Error: Unknown property [%s] for cluster %s" % (prop, cluster))

  return SimpleResult(Status.Ok)


# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  '''
  :param command:
  :param parser:
  :param args:
  :param unknown_args:
  :return:
  '''
  configcommand = cl_args.get('configcommand', None)
  if configcommand == 'set':
    return _set(cl_args)
  elif configcommand == 'unset':
    return _unset(cl_args)
  else:
    return _list(cl_args)
