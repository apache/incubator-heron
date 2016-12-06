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
'''response.py'''
import abc

from heron.common.src.python.utils.log import Log

# # pylint: disable=no-init
class Status(object):
  """Status code enum"""
  Ok, NonUserError, UserError = range(3)

class Response(object):
  """Response class that captures result of executing an action"""
  def __init__(self, status, succ_msg=None, err_msg=None, extra_msg=None):
    self.status = self.status_type(status)
    self.succ_msg = succ_msg
    self.err_msg = err_msg
    self.extra_msg = extra_msg

  # Meaning of exit status code:
  #  - status code = 0:
  #    program exits without error
  #  - 0 < status code < 100:
  #    program fails to execute before program execution. For example,
  #    JVM cannot find or load main class
  #  - status code >= 100:
  #    program fails to launch after program execution. For example,
  #    topology definition file fails to be loaded
  @staticmethod
  def status_type(status_code):
    if status_code == 0:
      return Status.Ok
    elif status_code < 100:
      return Status.NonUserError
    else:
      return Status.UserError

  @abc.abstractmethod
  def render(self):
    pass

# Response from the action of loading topology definition file
class TopologyDefLoadResponse(Response):
  """Response class that captures result of loading topology definition"""
  def __init__(self, status=1, defn_file=None, succ_msg=None, err_msg=None, extra_msg=None):
    super(TopologyDefLoadResponse, self).__init__(status, succ_msg, err_msg, extra_msg)
    self.defn_file = defn_file

  def render(self):
    if self.status != Status.Ok:
      if self.err_msg:
        Log.error(self.err_msg)
      else:
        Log.error("Unable to load topology definition file: %s", self.defn_file)
      if self.extra_msg:
        Log.debug(self.extra_msg)

# Response from shelled-out process
# pylint: disable=abstract-method
class InvocationResponse(Response):
  """Response class that captures result of a shelled-out program"""
  def __init__(self, main_class, topo_type, status, succ_msg, err_msg, extra_msg):
    super(InvocationResponse, self).__init__(status, succ_msg, err_msg, extra_msg)
    self.main_class = main_class
    self.topo_type = topo_type

# Response from shelled-out process that creates topology definition file
class TopologyDefCreationResponse(InvocationResponse):
  """Response class that captures result of the program that creates topology definition file"""
  def __init__(self, topology_file, main_class, topo_type, status, succ_msg, err_msg, extra_msg):
    super(TopologyDefCreationResponse, self).__init__(
        main_class, topo_type, status, succ_msg, err_msg, extra_msg)
    self.topology_file = topology_file

  def render(self):
    if self.status != Status.Ok:
      Log.error("Unable to create %s topology definition file '%s' by invoking'%s'",
                self.topo_type, self.topology_file, self.main_class)
      Log.error(self.extra_msg)

# Response from shelled-out process that launches topology
class TopologyLaunchResponse(InvocationResponse):
  """Response class that captures result of the program that launches topology"""
  def __init__(self, main_class, topo_type, topo_name, status, succ_msg, err_msg, extra_msg):
    super(TopologyLaunchResponse, self).__init__(
        main_class, topo_type, status, succ_msg, err_msg, extra_msg)
    self.topo_name = topo_name

  def render(self):
    if self.status == Status.Ok:
      Log.info("Successfully launched topology '%s'", self.topo_name)
    else:
      Log.error("Failed to launch %s topology '%s'", self.topo_type, self.topo_name)
      if self.status == Status.NonUserError:
        Log.error(self.extra_msg)
      else:
        Log.error(self.err_msg)
        Log.debug(self.extra_msg)

def render(resp):
  if isinstance(resp, list):
    for r in resp:
      r.render()
  elif isinstance(resp, Response):
    resp.render()
  else:
    raise RuntimeError("Unknown response type")

