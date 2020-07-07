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

''' version.py '''
import heron.tools.common.src.python.utils.config as config
import heron.tools.explorer.src.python.args as args


def create_parser(subparsers):
  """ create parser """
  parser = subparsers.add_parser(
      'version',
      help='Display version',
      usage="%(prog)s",
      add_help=False)
  args.add_titles(parser)
  parser.set_defaults(subcommand='version')
  return parser

# pylint: disable=unused-argument
def run(command, parser, known_args, unknown_args):
  """ run command """
  config.print_build_info()
  return True
