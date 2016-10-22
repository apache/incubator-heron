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

#!/usr/bin/env python2.7
''' main.py '''
import argparse
import sys
import time

import heron.common.src.python.utils.log as log
import heron.tools.common.src.python.utils.config as config
import heron.tools.explorer.src.python.args as parse
import heron.tools.explorer.src.python.clusters as clusters
# pylint: disable=redefined-builtin
import heron.tools.explorer.src.python.help as help
import heron.tools.explorer.src.python.logicalplan as logicalplan
import heron.tools.explorer.src.python.opts as opts
import heron.tools.explorer.src.python.physicalplan as physicalplan
import heron.tools.explorer.src.python.topologies as topologies
import heron.tools.explorer.src.python.version as version

Log = log.Log

# pylint: disable=bad-super-call
class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  """ subcommand help message formatter """
  def _format_action(self, action):
    parts = super(argparse.RawDescriptionHelpFormatter,
                  self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts


################################################################################
# Main parser
################################################################################
def create_parser():
  """ create parser """
  help_epilog = '''Getting more help:
  heron-explorer help <command>     Disply help and options for <command>\n
  For detailed documentation, go to http://heronstreaming.io'''

  parser = argparse.ArgumentParser(
      prog='heron-explorer',
      epilog=help_epilog,
      formatter_class=SubcommandHelpFormatter,
      add_help=False)

  # sub-commands
  subparsers = parser.add_subparsers(
      title="Available commands",
      metavar='<command> <options>')

  # subparser for subcommands related to clusters
  clusters.create_parser(subparsers)

  # subparser for subcommands related to logical plan
  logicalplan.create_parser(subparsers)

  # subparser for subcommands related to physical plan
  physicalplan.create_parser(subparsers)

  # subparser for subcommands related to displaying info
  topologies.create_parser(subparsers)

  # subparser for help subcommand
  help.create_parser(subparsers)

  # subparser for version subcommand
  version.create_parser(subparsers)

  return parser


################################################################################
# Run the command
################################################################################
# pylint: disable=too-many-return-statements
def run(command, *args):
  """ run command """
  # show all clusters
  if command == 'clusters':
    return clusters.run(command, *args)

  # show topologies
  elif command == 'topologies':
    return topologies.run(command, *args)

  # physical plan
  elif command == 'containers':
    return physicalplan.run_containers(command, *args)
  elif command == 'metrics':
    return physicalplan.run_metrics(command, *args)

  # logical plan
  elif command == 'components':
    return logicalplan.run_components(command, *args)
  elif command == 'spouts':
    return logicalplan.run_spouts(command, *args)
  elif command == 'bolts':
    return logicalplan.run_bolts(command, *args)

  # help
  elif command == 'help':
    return help.run(command, *args)

  # version
  elif command == 'version':
    return version.run(command, *args)

  return 1


def extract_common_args(command, parser, cl_args):
  """ extract common args """
  try:
    # do not pop like cli because ``topologies`` subcommand still needs it
    cluster_role_env = cl_args['cluster/[role]/[env]']
    config_path = cl_args['config_path']
  except KeyError:
    # if some of the arguments are not found, print error and exit
    subparser = config.get_subparser(parser, command)
    print subparser.format_help()
    return dict()
  cluster = config.get_heron_cluster(cluster_role_env)
  config_path = config.get_heron_cluster_conf_dir(cluster, config_path)

  new_cl_args = dict()
  try:
    cluster_tuple = config.parse_cluster_role_env(cluster_role_env, config_path)
    new_cl_args['cluster'] = cluster_tuple[0]
    new_cl_args['role'] = cluster_tuple[1]
    new_cl_args['environ'] = cluster_tuple[2]
    new_cl_args['config_path'] = config_path
  except Exception as e:
    Log.error("Unable to get valid topology location: %s", str(e))
    return dict()

  cl_args.update(new_cl_args)
  return cl_args


################################################################################
# Run the command
################################################################################
def main(args):
  """ main """
  # create the argument parser
  parser = create_parser()

  # if no argument is provided, print help and exit
  if not args:
    parser.print_help()
    return 0

  # insert the boolean values for some of the options
  all_args = parse.insert_bool_values(args)

  # parse the args
  args, unknown_args = parser.parse_known_args(args=all_args)
  command_line_args = vars(args)
  command = command_line_args['subcommand']

  if unknown_args:
    Log.error('Unknown argument: %s', unknown_args[0])
    # show help message
    command_line_args['help-command'] = command
    command = 'help'

  if command not in ['help', 'version']:
    opts.set_tracker_url(command_line_args)
    log.set_logging_level(command_line_args)
    if command not in ['topologies', 'clusters']:
      command_line_args = extract_common_args(command, parser, command_line_args)
    if not command_line_args:
      return 1
    Log.info("Using tracker URL: %s", command_line_args["tracker_url"])

  # timing command execution
  start = time.time()
  ret = run(command, parser, command_line_args, unknown_args)
  end = time.time()

  if command != 'help':
    sys.stdout.flush()
    Log.info('Elapsed time: %.3fs.', (end - start))

  return 0 if ret else 1

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
