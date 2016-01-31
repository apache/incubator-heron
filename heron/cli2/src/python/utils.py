#!/usr/bin/env python2.7

import argparse
import sys
import subprocess
import os

################################################################################
# Retrieve the given subparser from parser
################################################################################
def get_subparser(parser, command):
  subparsers_actions = [
    action for action in parser._actions
    if isinstance(action, argparse._SubParsersAction)
  ]

  # there will probably only be one subparser_action,
  # but better save than sorry
  for subparsers_action in subparsers_actions:
    # get all subparsers 
    for choice, subparser in subparsers_action.choices.items():
      if choice == command:
        return subparser
  return None

################################################################################
# Get normalized class path depending on platform
################################################################################
def identity(x):
  return x

def cygpath(x):
  command = ['cygpath', '-wp', x]
  p = subprocess.Popen(command,stdout=subprocess.PIPE)
  output, errors = p.communicate()
  lines = output.split("\n")
  return lines[0]

def normalized_class_path(x):
  if sys.platform == 'cygwin':
    return cygpath(x)
  return identity(x)

################################################################################
# Get normalized class path depending on platform
################################################################################
def get_heron_dir():
  """
  This will extract heron directory from .pex file.
  :return: root location for heron-cli.
  """
  path = "/".join(os.path.realpath( __file__ ).split('/')[:-7])
  return normalized_class_path(path)


