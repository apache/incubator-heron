#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

''' utils.py '''
import functools
import grp
import os
import pkgutil
import pwd
import stat
import subprocess

from datetime import datetime
from xml.sax.saxutils import escape

from heron.common.src.python.utils import proc

def format_mode(sres):
  """
  Format a line in the directory list based on the file's type and other attributes.
  """
  mode = sres.st_mode

  root = (mode & 0o700) >> 6
  group = (mode & 0o070) >> 3
  user = (mode & 0o7)

  def stat_type(md):
    ''' stat type'''
    if stat.S_ISDIR(md):
      return 'd'
    elif stat.S_ISSOCK(md):
      return 's'
    else:
      return '-'

  def triple(md):
    ''' triple '''
    return '%c%c%c' % (
        'r' if md & 0b100 else '-',
        'w' if md & 0b010 else '-',
        'x' if md & 0b001 else '-')

  return ''.join([stat_type(mode), triple(root), triple(group), triple(user)])

def format_mtime(mtime):
  """
  Format the date associated with a file to be displayed in directory listing.
  """
  now = datetime.now()
  dt = datetime.fromtimestamp(mtime)
  return '%s %2d %5s' % (
      dt.strftime('%b'), dt.day,
      dt.year if dt.year != now.year else dt.strftime('%H:%M'))

# pylint: disable=unused-argument
def format_prefix(filename, sres):
  """
  Prefix to a filename in the directory listing. This is to make the
  listing similar to an output of "ls -alh".
  """
  try:
    pwent = pwd.getpwuid(sres.st_uid)
    user = pwent.pw_name
  except KeyError:
    user = sres.st_uid

  try:
    grent = grp.getgrgid(sres.st_gid)
    group = grent.gr_name
  except KeyError:
    group = sres.st_gid

  return '%s %3d %10s %10s %10d %s' % (
      format_mode(sres),
      sres.st_nlink,
      user,
      group,
      sres.st_size,
      format_mtime(sres.st_mtime),
  )

def get_listing(path):
  """
  Returns the list of files and directories in a path.
  Prepents a ".." (parent directory link) if path is not current dir.
  """
  if path != ".":
    listing = sorted(['..'] + os.listdir(path))
  else:
    listing = sorted(os.listdir(path))
  return listing

def get_stat(path, filename):
  ''' get stat '''
  return os.stat(os.path.join(path, filename))

def read_chunk(filename, offset=-1, length=-1, escape_data=False):
  """
  Read a chunk of a file from an offset upto the length.
  """
  try:
    length = int(length)
    offset = int(offset)
  except ValueError:
    return {}

  if not os.path.isfile(filename):
    return {}

  try:
    fstat = os.stat(filename)
  except Exception:
    return {}

  if offset == -1:
    offset = fstat.st_size

  if length == -1:
    length = fstat.st_size - offset

  with open(filename, "r") as fp:
    fp.seek(offset)
    try:
      data = fp.read(length)
    except IOError:
      return {}

  if data:
    data = _escape_data(data) if escape_data else data
    return dict(offset=offset, length=len(data), data=data)

  return dict(offset=offset, length=0)

def _escape_data(data):
  return escape(data.decode('utf8', 'replace'))

def pipe(prev_proc, to_cmd):
  """
  Pipes output of prev_proc into to_cmd.
  Returns piped process
  """
  stdin = None if prev_proc is None else prev_proc.stdout
  process = subprocess.Popen(to_cmd,
                             stdout=subprocess.PIPE,
                             stdin=stdin)
  if prev_proc is not None:
    prev_proc.stdout.close() # Allow prev_proc to receive a SIGPIPE
  return process

def str_cmd(cmd, cwd, env):
  """
  Runs the command and returns its stdout and stderr.
  """
  process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, cwd=cwd, env=env)
  stdout_builder, stderr_builder = proc.async_stdout_stderr_builder(process)
  process.wait()
  stdout, stderr = stdout_builder.result(), stderr_builder.result()
  return {'command': ' '.join(cmd), 'stderr': stderr, 'stdout': stdout}

# pylint: disable=unnecessary-lambda
def chain(cmd_list):
  """
  Feed output of one command to the next and return final output
  Returns string output of chained application of commands.
  """
  command = ' | '.join([' '.join(x) for x in cmd_list])
  chained_proc = functools.reduce(pipe, [None] + cmd_list)
  stdout_builder = proc.async_stdout_builder(chained_proc)
  chained_proc.wait()
  return {
      'command': command,
      'stdout': stdout_builder.result()
  }

def get_container_id(instance_id):
  ''' get container id '''
  return instance_id.split('_')[1]  # Format: container_<index>_component_name_<index>

def get_asset(asset_name):
  ''' get assset '''
  return pkgutil.get_data("heron.shell", os.path.join("assets", asset_name))

def check_path(path):
  """
  file path should be a relative path without ".." in it
  :param path: file path
  :return: true if the path is relative and doesn't contain ".."
  """
  return not path.startswith("/") and ".." not in path
