#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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
''' kill.py '''
from heron.common.src.python.utils.log import Log
import heron.tools.cli.src.python.cli_helper as cli_helper

def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = cli_helper.create_parser(subparsers, 'kill', 'Kill a topology')

  parser.add_argument(
      'skip-runtime-validation',
      default=False,
      help='Skip runtime validation. This option should only be used when the topology is running '
           'in an unexpected state and can\'t be killed without this option')

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
  Log.debug("Kill Args: %s", cl_args)
  skip_runtime_validation = cl_args['skip-runtime-validation']

  if cl_args['deploy_mode'] == config.SERVER_MODE:
    dict_extra_args = {"skip_runtime_validation": str(skip_runtime_validation)}
    return cli_helper.run_server(command, cl_args, "kill topology", extra_args=dict_extra_args)
  else:
    list_extra_args = ["--skip_runtime_validation", str(skip_runtime_validation)]
    return cli_helper.run_direct(command, cl_args, "kill topology", extra_args=list_extra_args)
