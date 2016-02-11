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
      'classpath', 
      help='Print class path of heron-cli',
      usage = "%(prog)s",
      add_help = False)

  args.add_titles(parser)

  parser.set_defaults(subcommand='classpath')
  return parser

def run(command, parser, args, unknown_args):
  pass
