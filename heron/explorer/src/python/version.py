# copyright 2016 twitter. all rights reserved.
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#    http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

import heron.explorer.src.python.args as args
import heron.common.src.python.utils as utils


def create_parser(subparsers):
  parser = subparsers.add_parser(
      'version',
      help='Display version',
      usage="%(prog)s",
      add_help=False)
  args.add_titles(parser)
  parser.set_defaults(subcommand='version')
  return parser


def run(command, parser, args, unknown_args):
  release_file = utils.get_heron_release_file()
  with open(release_file) as release_info:
    for line in release_info:
        print line,
  return True
