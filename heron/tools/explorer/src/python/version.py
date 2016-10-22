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
