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
      'deactivate', 
      help='Deactivate a topology',
      usage = "%(prog)s [options] config-overrides topology-name",
      add_help = False)

  args.add_titles(parser)
  args.add_config_overrides(parser)
  args.add_topology(parser)

  args.add_config(parser)
  args.add_verbose(parser)

  parser.set_defaults(subcommand='deactivate')
  return parser

def execute(parser, args, unknown_args):
  pass
