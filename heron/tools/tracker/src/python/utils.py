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
'''
utils.py
Contains utility functions used by tracker.
'''

import os
import string
import sys
import subprocess

# directories for heron tools distribution
BIN_DIR = "bin"
CONF_DIR = "conf"
LIB_DIR = "lib"

def hex_escape(bin_str):
  """
  Hex encode a binary string
  """
  printable = string.ascii_letters + string.digits + string.punctuation + ' '
  return ''.join(ch if ch in printable else r'0x{0:02x}'.format(ord(ch)) for ch in bin_str)

def make_shell_endpoint(topologyInfo, instance_id):
  """
  Makes the http endpoint for the heron shell
  if shell port is present, otherwise returns None.
  """
  # Format: container_<id>_<instance_id>
  pplan = topologyInfo["physical_plan"]
  stmgrId = pplan["instances"][instance_id]["stmgrId"]
  host = pplan["stmgrs"][stmgrId]["host"]
  shell_port = pplan["stmgrs"][stmgrId]["shell_port"]
  return "http://%s:%d" % (host, shell_port)

def make_shell_job_url(host, shell_port, _):
  """
  Make the job url from the info
  stored in stmgr. This points to dir from where
  all the processes are started.
  If shell port is not present, it returns None.
  """
  if not shell_port:
    return None
  return "http://%s:%d/browse/" % (host, shell_port)

def make_shell_logfiles_url(host, shell_port, _, instance_id=None):
  """
  Make the url for log-files in heron-shell
  from the info stored in stmgr.
  If no instance_id is provided, the link will
  be to the dir for the whole container.
  If shell port is not present, it returns None.
  """
  if not shell_port:
    return None
  if not instance_id:
    return "http://%s:%d/browse/log-files" % (host, shell_port)
  else:
    return "http://%s:%d/file/log-files/%s.log.0" % (host, shell_port, instance_id)

def make_shell_logfile_data_url(host, shell_port, instance_id, offset, length):
  """
  Make the url for log-file data in heron-shell
  from the info stored in stmgr.
  """
  return "http://%s:%d/filedata/log-files/%s.log.0?offset=%s&length=%s" % \
    (host, shell_port, instance_id, offset, length)

def make_shell_filestats_url(host, shell_port, path):
  """
  Make the url for filestats data in heron-shell
  from the info stored in stmgr.
  """
  return "http://%s:%d/filestats/%s" % (host, shell_port, path)

# pylint: disable=unused-argument
def make_viz_dashboard_url(name, cluster, environ):
  """
  Link to the dashboard. Must override to return a valid url.
  """
  return ""

################################################################################
# Get normalized class path depending on platform
################################################################################
def identity(x):
  """
  This will return the input arg
  :return: input argument
  """
  return x

def cygpath(x):
  """
  This will return the path of input arg for windows
  :return: the path in windows
  """
  command = ['cygpath', '-wp', x]
  p = subprocess.Popen(command, stdout=subprocess.PIPE)
  output, _ = p.communicate()
  lines = output.split("\n")
  return lines[0]

def normalized_class_path(x):
  """
  This will return the class path depending on the platform
  :return: the class path
  """
  if sys.platform == 'cygwin':
    return cygpath(x)
  return identity(x)

################################################################################
# Get the root of heron tracker dir and various sub directories
################################################################################
def get_heron_tracker_dir():
  """
  This will extract heron tracker directory from .pex file.
  :return: root location for heron-tools.
  """
  path = "/".join(os.path.realpath(__file__).split('/')[:-8])
  return normalized_class_path(path)

def get_heron_tracker_bin_dir():
  """
  This will provide heron tracker bin directory from .pex file.
  :return: absolute path of heron lib directory
  """
  bin_path = os.path.join(get_heron_tracker_dir(), BIN_DIR)
  return bin_path

def get_heron_tracker_conf_dir():
  """
  This will provide heron tracker conf directory from .pex file.
  :return: absolute path of heron conf directory
  """
  conf_path = os.path.join(get_heron_tracker_dir(), CONF_DIR)
  return conf_path

# def get_heron_tracker_lib_dir():
#   """
#   This will provide heron tracker lib directory from .pex file.
#   :return: absolute path of heron lib directory
#   """
#   lib_path = os.path.join(get_heron_tools_dir(), LIB_DIR)
#   return lib_path
