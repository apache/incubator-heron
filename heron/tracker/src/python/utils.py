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

"""
Contains utility functions used by tracker.
"""

import os
import string
import sys

from heron.proto.execution_state_pb2 import ExecutionState

# directories for heron tools distribution
BIN_DIR  = "bin"
CONF_DIR = "conf"
LIB_DIR  = "lib"

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
  component_id = instance_id.split('_')[1]
  pplan = topologyInfo["physical_plan"]
  stmgrId = pplan["instances"][instance_id]["stmgrId"]
  host = pplan["stmgrs"][stmgrId]["host"]
  shell_port = pplan["stmgrs"][stmgrId]["shell_port"]
  return "http://%s:%d" % (host, shell_port)

def make_shell_job_url(host, shell_port, cwd):
  """
  Make the job url from the info
  stored in stmgr. This points to dir from where
  all the processes are started.
  If shell port is not present, it returns None.
  """
  if not shell_port:
    return None
  return "http://%s:%d/browse/" % (host, shell_port)

def make_shell_logfiles_url(host, shell_port, cwd, instance_id=None):
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
  return "http://%s:%d/filedata/log-files/%s.log.0?offset=%s&length=%s" % (host, shell_port, instance_id, offset, length)

def make_shell_filestats_url(host, shell_port, path):
  """
  Make the url for filestats data in heron-shell
  from the info stored in stmgr.
  """
  return "http://%s:%d/filestats/%s" % (host, shell_port, path)

def make_viz_dashboard_url(name, cluster, environ):
  """
  Link to the dashboard. Must override to return a valid url.
  """
  return ""

def convert_execution_state(execution_state):
  """
  Old version of execution state was specific to aurora.
  This method is for backward compatibility.
  Converts the old execution state to new one
  and returns it.
  If execution_state is the new one, simply returns it.
  """
  if not execution_state.HasField("aurora"):
    # "dc" field is deprecated, so convert that to "cluster" if present.
    if execution_state.HasField("dc"):
      execution_state.cluster = execution_state.dc
    return execution_state

  aurora = execution_state.aurora
  job = aurora.jobs[0]
  dc = job.dc
  environ = job.environ
  role = job.user
  release_username = aurora.release_username
  release_tag = aurora.release_tag
  release_version = aurora.release_version
  uploader_version = aurora.packer_version

  estate = ExecutionState()
  estate.topology_name = execution_state.topology_name
  estate.topology_id = execution_state.topology_id
  estate.submission_time = execution_state.submission_time
  estate.submission_user = execution_state.submission_user
  estate.release_state.release_username = release_username
  estate.release_state.release_tag = release_tag
  estate.release_state.release_version = release_version
  estate.release_state.uploader_version = uploader_version
  estate.cluster = dc
  estate.environ = environ
  estate.role = role
  assert estate.IsInitialized()
  return estate


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
  p = subprocess.Popen(command,stdout=subprocess.PIPE)
  output, errors = p.communicate()
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
  path = "/".join(os.path.realpath( __file__ ).split('/')[:-7])
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

def get_heron_tracker_lib_dir():
  """
  This will provide heron tracker lib directory from .pex file.
  :return: absolute path of heron lib directory
  """
  lib_path = os.path.join(get_heron_tools_dir(), LIB_DIR)
  return lib_path
