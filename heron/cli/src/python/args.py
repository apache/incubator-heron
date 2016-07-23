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
''' args.py '''
import os

import heron.common.src.python.utils as utils


def add_titles(parser):
  '''
  :param parser:
  :return:
  '''
  # pylint: disable=protected-access
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser


def insert_bool(param, command_args):
  '''
  :param param:
  :param command_args:
  :return:
  '''
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
  '''
  :param command_line_args:
  :return:
  '''
  args1 = insert_bool('--verbose', command_line_args)
  args2 = insert_bool('--deploy-deactivated', args1)
  return args2


def add_verbose(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--verbose',
      metavar='(a boolean; default: "false")',
      default=False)
  return parser


def add_trace_execution(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--trace-execution',
      metavar='(a boolean; default: "false")',
      default=False)
  return parser


def add_topology(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      'topology-name',
      help='Name of the topology')
  return parser


def add_topology_file(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      'topology-file-name',
      help='Topology jar/tar/zip file')
  return parser


def add_topology_class(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      'topology-class-name',
      help='Topology class name')
  return parser


def add_cluster_role_env(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      'cluster/[role]/[env]',
      help='Cluster, role, and environment to run topology'
  )
  return parser


def add_config(parser):
  '''
  :param parser:
  :return:
  '''
  # the default config path
  default_config_path = utils.get_heron_conf_dir()

  parser.add_argument(
      '--config-path',
      metavar='(a string; path to cluster config; default: "' + default_config_path + '")',
      default=os.path.join(utils.get_heron_dir(), default_config_path))

  parser.add_argument(
      '--config-property',
      metavar='(key=value; a config key and its value; default: [])',
      action='append',
      default=[])
  return parser


def add_system_property(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--topology-main-jvm-property',
      metavar='(property=value; JVM system property for executing topology main; default: [])',
      action="append",
      default=[])

  return parser


def add_deactive_deploy(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--deploy-deactivated',
      metavar='(a boolean; default: "false")',
      default=False)
  return parser
