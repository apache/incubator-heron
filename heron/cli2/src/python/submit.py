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

import heron.cli2.src.python.args as args

def create_parser(subparsers):
  parser = subparsers.add_parser(
      'submit', 
      help='Submit a topology',
      usage = "%(prog)s [options] config-overrides topology-file-name topology-class-name [topology-args]",
      add_help = False)

  args.add_titles(parser)
  args.add_config_overrides(parser)
  args.add_topology_file(parser)
  args.add_topology_class(parser)
  args.add_config(parser)

  parser.add_argument(
      '--deploy-deactivated', 
      metavar='(a boolean; default: "false")',
      default=False)

  args.add_verbose(parser)

  parser.set_defaults(subcommand='submit')
  return parser

def execute(parser, args, unknown_args):
  pass
