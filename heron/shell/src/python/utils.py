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
''' utils.py '''
import grp
import os
import pwd
import stat

from datetime import datetime
from xml.sax.saxutils import escape

def format_mode(sres):
  """
  Format a line in the directory list based on the file's type and other attributes.
  """
  mode = sres.st_mode

  root = (mode & 0700) >> 6
  group = (mode & 0070) >> 3
  user = (mode & 07)

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

def read_chunk(filename, offset=None, length=None):
  """
  Read a chunk of a file from an offset upto the length.
  """
  offset = offset or -1
  length = length or -1

  try:
    length = long(length)
    offset = long(offset)
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
    return dict(offset=offset, length=len(data), data=escape(data.decode('utf8', 'replace')))

  return dict(offset=offset, length=0)
