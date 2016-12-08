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
from heron.common.src.python.utils.log import Log

# pylint: disable=no-init
class Status(object):
  """Status code enum"""
  Ok, HeronError, InvocationError = range(3)

# Meaning of exit status code:
#  - status code = 0:
#    program exits without error
#  - 0 < status code < 100:
#    program fails to execute before program execution. For example,
#    JVM cannot find or load main class
#  - status code >= 100:
#    program fails to launch after program execution. For example,
#    topology definition file fails to be loaded
def status_type(status_code):
  if status_code == 0:
    return Status.Ok
  elif status_code < 100:
    return Status.InvocationError
  else:
    return Status.HeronError

class Response(object):
  """Response class that captures result of executing an action"""
  def __init__(self, status_code, msg=None, detailed_msg=None):
    self.status = status_type(status_code)
    self.msg = msg
    self.detailed_msg = detailed_msg

  def add_context(self, err_context, succ_context=None):
    """ Prepend msg to add some context information

    :param pmsg: context info
    :return: None
    """
    if self.status != Status.Ok:
      self.msg = "%s : %s" % (err_context, self.msg) if self.msg else err_context
    else:
      if succ_context is not None:
        self.msg = "%s : %s" % (succ_context, self.msg) if self.msg else succ_context

def render(resp):
  if isinstance(resp, list):
    for r in resp:
      render(r)
  elif isinstance(resp, Response):
    if resp.status == Status.Ok:
      if resp.msg:
        Log.info(resp.msg)
      if resp.detailed_msg:
        Log.debug(resp.detailed_msg)
    elif resp.status == Status.HeronError:
      if resp.msg:
        Log.error(resp.msg)
      if resp.detailed_msg:
        Log.debug(resp.detailed_msg)
    else:
      Log.error(resp.detailed_msg)
  else:
    raise RuntimeError("Unknown response instance")
