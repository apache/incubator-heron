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
''' clusters.py '''
import heron.explorer.src.python.args as args
# from heron.common.src.python.color import Log
# from tabulate import tabulate
import heron.common.src.python.utils as utils


def create_parser(subparsers):
  """ create argument parser """
  parser = subparsers.add_parser(
      'clusters',
      help='Display exisitng clusters',
      usage="%(prog)s [options]",
      add_help=True)
  args.add_verbose(parser)
  args.add_tracker_url(parser)
  parser.set_defaults(subcommand='clusters')
  return subparsers

# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  """ run command """
  clusters = utils.get_clusters()
  print 'Available clusters:'
  for cluster in clusters:
    print '  %s' % cluster
  return True
