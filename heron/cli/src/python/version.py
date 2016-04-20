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

import heron.cli.src.python.args as args
import heron.cli.src.python.utils as utils
from heron.common.src.python.color import Log

def create_parser(subparsers):
  parser = subparsers.add_parser(
      'version', 
      help='Print version of heron-cli',
      usage = "%(prog)s",
      add_help = False)

  args.add_titles(parser)

  parser.set_defaults(subcommand='version')
  return parser

def run(command, parser, args, unknown_args):

  release_file = utils.get_heron_release_file()
  with open(release_file) as release_info:
    for line in release_info:
      if not "git" in line: 
        print line,

  return True
