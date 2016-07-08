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

import heron.explorer.src.python.utils as utils
import os


# default parameter - url to connect to heron tracker
DEFAULT_TRACKER_URL = "http://localhost:8888"


# add argument for config file path
def add_config(parser):

  # the default config path
  default_config_path = utils.get_heron_conf_dir()

  parser.add_argument(
      '--config-path',
      metavar='(a string; path to cluster config; default: "' + default_config_path + '")',
      default=os.path.join(utils.get_heron_dir(), default_config_path))

  return parser


def add_titles(parser):
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser


def insert_bool(param, command_args):
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
  args1 = insert_bool('--verbose', command_line_args)
  return args1


# add optional argument that sets log level to verbose
def add_verbose(parser):
  parser.add_argument(
      '--verbose',
      metavar='(a boolean; default: "false")',
      type=bool,
      default=False)
  return parser


# add optional argument that specifies tracker API endpoint
def add_tracker_url(parser):
  parser.add_argument(
    '--tracker_url',
    metavar='(tracker url; default: "' + DEFAULT_TRACKER_URL + '")',
    type=str, default=DEFAULT_TRACKER_URL)
  return parser


# add optional argument that specifies container id
def add_container_id(parser):
  parser.add_argument(
    '--cid',
    help='container ID',
    type=int, metavar='ID')
  return parser


# add optional argument that specifies component name
def add_component_name(parser):
  parser.add_argument(
    '--component',
    help='Component name',
    metavar='COMP',
    type=str)
  return parser


# add optional argument that displays spout only
def add_spouts(parser):
  parser.add_argument(
    '--spout', help='display spout', action='store_true')
  return parser


# add optional argument that displays bolts only
def add_bolts(parser):
  parser.add_argument(
    '--bolt', help='display bolt', action='store_true')
  return parser


# add argument that specifies topologies location
def add_cluster_role_env(parser):
  parser.add_argument(
    'cluster/[role]/[env]', help='Topologies location', type=str,
    metavar='CLUSTER/[ROLE]/[ENV]')
  return parser


# add argument that specifies topology name
def add_topology_name(parser):
  parser.add_argument(
    'topology-name',
    help='Topology name'
  )
  return parser
