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

''' args.py '''
import os
import heron.tools.common.src.python.utils.config as config

# default parameter - url to connect to heron tracker
DEFAULT_TRACKER_URL = "http://127.0.0.1:8888"


# add argument for config file path
def add_config(parser):
  """ add config """
  # the default config path
  default_config_path = config.get_heron_conf_dir()

  parser.add_argument(
      '--config-path',
      metavar='(a string; path to cluster config; default: "' + default_config_path + '")',
      default=os.path.join(config.get_heron_dir(), default_config_path))

  return parser


# pylint: disable=protected-access
def add_titles(parser):
  """ add titles """
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser


def insert_bool(param, command_args):
  """ insert boolean """
  index = 0
  found = False
  for lelem in command_args:
    if lelem == '--' and not found:
      break
    if lelem == param:
      found = True
      break
    index = index + 1

  if found:
    command_args.insert(index + 1, 'True')
  return command_args


def insert_bool_values(command_line_args):
  """ insert boolean values """
  args1 = insert_bool('--verbose', command_line_args)
  return args1


# add optional argument that sets log level to verbose
def add_verbose(parser):
  """ add optional verbose argument"""
  parser.add_argument(
      '--verbose',
      metavar='(a boolean; default: "false")',
      type=bool,
      default=False)
  return parser


def add_tracker_url(parser):
  """ add optional tracker_url argument """
  parser.add_argument(
      '--tracker_url',
      metavar='(tracker url; default: "' + DEFAULT_TRACKER_URL + '")',
      type=str, default=DEFAULT_TRACKER_URL)
  return parser


def add_container_id(parser):
  """ add optional argument that specifies container id """
  parser.add_argument(
      '--id',
      help='container ID',
      type=int, metavar='ID')
  return parser


def add_component_name(parser):
  """ add optional argument that specifies component name """
  parser.add_argument(
      '--component',
      help='Component name',
      metavar='COMP',
      type=str)
  return parser


def add_spouts(parser):
  """ add optional argument that displays spout only """
  parser.add_argument(
      '--spout', help='display spout', action='store_true')
  return parser


def add_bolts(parser):
  """ add optional argument that displays bolts only """
  parser.add_argument(
      '--bolt', help='display bolt', action='store_true')
  return parser


def add_cluster_role_env(parser):
  """ add argument that specifies topologies location """
  parser.add_argument(
      'cluster/[role]/[env]', help='Topologies location', type=str,
      metavar='CLUSTER/[ROLE]/[ENV]')
  return parser


def add_topology_name(parser):
  """ add argument that specifies topology name """
  parser.add_argument(
      'topology-name',
      help='Topology name'
  )
  return parser
