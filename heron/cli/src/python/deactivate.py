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

#!/usr/bin/python2.7

import argparse
import atexit
import base64
import contextlib
import glob
import logging
import logging.handlers
import os
import shutil
import sys
import subprocess
import tarfile
import tempfile

from heron.common.src.python.color import Log

import heron.cli.src.python.args as args
import heron.cli.src.python.execute as execute
import heron.cli.src.python.jars as jars
import heron.cli.src.python.opts as opts
import heron.cli.src.python.utils as utils

def create_parser(subparsers):
  parser = subparsers.add_parser(
      'deactivate',
      help='Deactivate a topology',
      usage = "%(prog)s [options] cluster/[role]/[env] topology-name",
      add_help = False)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  args.add_config(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='deactivate')
  return parser

def run(command, parser, cl_args, unknown_args):

  try:
    topology_name = cl_args['topology-name']
    config_overrides = utils.parse_cmdline_override(cl_args)

    new_args = [
        "--cluster", cl_args['cluster'],
        "--role", cl_args['role'],
        "--environment", cl_args['environ'],
        "--heron_home", utils.get_heron_dir(),
        "--config_path", cl_args['config_path'],
        "--config_overrides", base64.b64encode(config_overrides),
        "--release_file", utils.get_heron_release_file(),       
        "--topology_name", topology_name,
        "--command", command,
    ]

    if opts.verbose():
      new_args.append("--verbose")

    lib_jars = utils.get_heron_libs(jars.scheduler_jars() + jars.statemgr_jars())

    # invoke the runtime manager to kill the topology
    execute.heron_class(
        'com.twitter.heron.scheduler.RuntimeManagerMain',
        lib_jars,
        extra_jars=[],
        args= new_args
    )

  except Exception as ex:
    print 'Error: %s' % str(ex)
    Log.error('Failed to deactivate topology \'%s\'' % topology_name)
    return False

  Log.info('Successfully deactivated topology \'%s\'' % topology_name)
  return True
