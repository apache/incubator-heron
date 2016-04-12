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

import heron.cli.src.python.args as args
import heron.cli.src.python.execute as execute
import heron.cli.src.python.jars as jars
import heron.cli.src.python.utils as utils

def create_parser(subparsers):
  parser = subparsers.add_parser(
      'config', 
      help='Config related commands',
      usage = "%(prog)s [options] cluster/[role]/[env]",
      add_help = False)

  args.add_titles(parser)
  args.add_cluster_role_env(parser)

  args.add_config(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='config')
  return parser

def run(command, parser, cl_args, unknown_args):
  # check if the required arguments are provided
  try:
    cluster_role_env = cl_args['cluster/[role]/[env]']

  except KeyError:
    subparser = utils.get_subparser(parser, command)
    print(subparser.format_help())
    parser.exit()

  try:
    cluster = utils.get_heron_cluster(cluster_role_env)
    config_path = utils.get_heron_cluster_conf_dir(cluster, cl_args['config_path'])
    config_overrides = \
        utils.parse_cluster_role_env(cluster_role_env) + ' ' + \
        utils.parse_cmdline_override(cl_args)

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
