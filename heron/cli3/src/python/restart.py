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

import heron.cli3.src.python.args as args
import heron.cli3.src.python.execute as execute
import heron.cli3.src.python.jars as jars
import heron.cli3.src.python.utils as utils

def create_parser(subparsers):
  parser = subparsers.add_parser(
      'restart', 
      help='Restart a topology',
      usage = "%(prog)s [options] cluster/[role]/[env] topology-name [shard-identifier]",
      add_help = False)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology(parser)

  parser.add_argument(
      'shard-identifier', 
      nargs='?', 
      type=int, 
      default=-1, 
      help='Identifier of the shard to be restarted')

  args.add_config(parser)
  args.add_verbose(parser)
  args.add_trace_execution(parser)

  parser.set_defaults(subcommand='restart')
  return parser

def run(command, parser, cl_args, unknown_args):
  # check if the required arguments are provided
  try:
    cluster_role_env = cl_args['cluster/[role]/[env]']
    topology_name = cl_args['topology-name']

  except KeyError:
    subparser = utils.get_subparser(parser, command)
    print(subparser.format_help())
    parser.exit()

  try:
    config_overrides = \
        args.parse_cluster_role_env(cluster_role_env) + ' ' + \
        args.parse_cmdline_override(cl_args)

    new_args = [
        command,
        topology_name,
        cl_args['config_loader'],
        base64.b64encode(config_overrides),
        cl_args['config_path']
    ]

    execute.heron_class(
        'com.twitter.heron.scheduler.service.RuntimeManagerMain',
        utils.get_heron_libs(jars.scheduler_jars()),
        extra_jars=[],
        args= new_args
    )

  except Exception as ex:
    print 'Error: %s' % str(ex)
    print 'Failed to restart topology \'%s\'' % topology_name
    sys.exit(1)

  print 'Successfully restarted topology \'%s\'' % topology_name
  sys.exit(0)
