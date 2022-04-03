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
import argparse
import os
import sys

from heron.tools.common.src.python.utils import config


def add_titles(parser):
  '''
  :param parser:
  :return:
  '''
  # pylint: disable=protected-access
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser


def add_verbose(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--verbose',
      default=False,
      help='Verbose mode. Increases logging level to show debug messages')
  return parser

def add_verbose_gc(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--verbose_gc',
      default=False,
      action='store_true',
      help='Produce JVM GC logging')
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

def add_service_url(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--service-url',
      default="",
      help='API service end point')
  return parser

def add_config(parser):
  '''
  :param parser:
  :return:
  '''
  # the default config path
  default_config_path = config.get_heron_conf_dir()

  parser.add_argument(
      '--config-path',
      default=os.path.join(config.get_heron_dir(), default_config_path),
      help='Path to cluster configuration files')

  parser.add_argument(
      '--config-property',
      metavar='PROPERTY=VALUE',
      action='append',
      default=[],
      help='Configuration properties that overrides default options')
  return parser


def add_system_property(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--topology-main-jvm-property',
      metavar='PROPERTY=VALUE',
      action="append",
      default=[],
      help='JVM system property for executing topology main')

  return parser


def add_deactive_deploy(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--deploy-deactivated',
      default=False,
      help='Deploy topology in deactivated mode')
  return parser


def add_extra_launch_classpath(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--extra-launch-classpath',
      metavar='CLASS_PATH',
      default="",
      help='Additional JVM class path for launching topology')
  return parser

def add_release_yaml_file(parser):
  '''
  :param parser:
  :return:
  '''
  parser.add_argument(
      '--release-yaml-file',
      default="",
      help='YAML file that contains release info. Default value is the '
           'path of the release file in this CLI')
  return parser

def add_dry_run(parser):
  '''
  :param parser:
  :return:
  '''
  default_format = 'table'
  resp_formats = ['raw', 'table', 'colored_table', 'json']
  # pylint: disable=consider-using-f-string
  available_options = ', '.join(['%s' % opt for opt in resp_formats])

  def dry_run_resp_format(value):
    if value not in resp_formats:
      raise argparse.ArgumentTypeError(
          f'Invalid dry-run response format: {value}. Available formats: {available_options}')
    return value

  parser.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='Enable dry-run mode. Information about '
           'the command will print but no action will be taken on the topology')

  parser.add_argument(
      '--dry-run-format',
      metavar='DRY_RUN_FORMAT',
      default='colored_table' if sys.stdout.isatty() else 'table',
      type=dry_run_resp_format,
      help="The format of the dry-run output "\
           f"([{'|'.join(resp_formats)}], default={default_format}). "
           "Ignored when dry-run mode is not enabled")

  return parser
