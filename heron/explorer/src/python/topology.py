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

import logging
import tornado.gen
import tornado.ioloop
from heron.ui.src.python.handlers.access import heron as API
import heron.explorer.src.python.args as args
from heron.explorer.src.python.physicalplan import get_topology_info
from tabulate import tabulate
import json

LOG = logging.getLogger(__name__)

def create_parser(subparsers):
  parser = subparsers.add_parser(
    'topology',
    help='Show topology info',
    usage = "%(prog)s [options]",
    add_help = False)
  args.add_cluster_env_topo(parser)
  args.add_verbose(parser)
  parser.set_defaults(subcommand='topology')
  return parser

def run(command, parser, cl_args, unknown_args):
  print(cl_args)
  try:
    topo_loc = [cluster, env, topology] = cl_args['[cluster]/[env]/[topology]'].split('/')
  except Exception:
    LOG.error('Error: invalid topology location')
    return False
  result = get_topology_info(cluster, env, topology)
  print(json.dumps(result, indent=4))
  return True
