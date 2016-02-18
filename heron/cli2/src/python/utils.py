#!/usr/bin/env python2.7

import argparse
import sys
import subprocess
import os
import tarfile
import contextlib

################################################################################
# Create a tar file with a given set of files
################################################################################
def create_tar(tar_filename, files):
  with contextlib.closing(tarfile.open(tar_filename, 'w:gz')) as tar:
    for filename in files:
      if not os.path.isfile(filename):
        raise Exception("%s is not an existing file" % filename)
      tar.add(filename, arcname=os.path.basename(filename))

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
# Get the normalized class path of all jars
################################################################################
def get_classpath(jars):
  return ':'.join(map(normalized_class_path, jars))

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

################################################################################
# Get all the heron lib jars with the absolute paths
################################################################################
def get_heron_libs(local_jars):
  heron_dir = get_heron_dir()
  heron_libs = [os.path.join(heron_dir, f) for f in local_jars]
  return heron_libs
