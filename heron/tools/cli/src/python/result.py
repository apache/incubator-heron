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
'''result.py'''
import abc
import sys
from enum import Enum

from heron.common.src.python.utils.log import Log

# Meaning of exit status code:
#  - status code = 0:
#    program exits without error
#  - 0 < status code < 100:
#    program fails to execute before program execution. For example,
#    JVM cannot find or load main class
#  - 100 <= status code < 200:
#    program fails to launch after program execution. For example,
#    topology definition file fails to be loaded
#  - status code == 200:
#    program sends out dry-run response

# Definition corresponds to definition in com.twitter.heron.scheduler.AbstractMain

# pylint: disable=no-init
class Status(Enum):
  """Status code enum"""
  Ok = 0
  InvocationError = 1
  HeronError = 100
  DryRun = 200

def status_type(status_code):
  if status_code == 0:
    return Status.Ok
  elif status_code < 100:
    return Status.InvocationError
  elif status_code == 200:
    return Status.DryRun
  else:
    return Status.HeronError

class Result(object):
  """Result class"""
  def __init__(self, status=None, err_context=None, succ_context=None):
    self.status = status
    self.err_context = err_context
    self.succ_context = succ_context

  @staticmethod
  def _do_log(log_f, msg):
    if msg:
      log_f(msg)

  def _log_context(self):
    if self.status == Status.Ok or self.status == Status.DryRun:
      self._do_log(Log.info, self.succ_context)
    elif self.status == Status.HeronError:
      self._do_log(Log.error, self.err_context)
    elif self.status == Status.InvocationError:
      # invocation error has no context
      pass
    else:
      raise RuntimeError(
          "Unknown status type of value %d. Expected value: %s", self.status.value, list(Status))

  def add_context(self, err_context, succ_context=None):
    """ Prepend msg to add some context information

    :param pmsg: context info
    :return: None
    """
    self.err_context = err_context
    self.succ_context = succ_context

  def is_successful(self):
    return self.status == Status.Ok

  @abc.abstractmethod
  def render(self):
    pass


class SimpleResult(Result):
  """Simple result: result that already and only
     contains status of the result"""
  def __init__(self, status):
    super(SimpleResult, self).__init__(status)

  def render(self):
    self._log_context()


class ProcessResult(Result):
  """Process result: a wrapper of result class"""
  def __init__(self, proc):
    super(ProcessResult, self).__init__()
    self.proc = proc

  def render(self):
    while True:
      stderr_line = self.proc.stderr.readline()
      if not stderr_line:
        if self.proc.poll() is None:
          continue
        stdout_line = self.proc.stdout.readline()
        if not stdout_line:
          if self.proc.poll() is None:
            continue
          else:
            break
        else:
          print >> sys.stdout, stdout_line[:-1]
      else:
        print >> sys.stderr, stderr_line[:-1]
    self.proc.wait()
    self.status = status_type(self.proc.returncode)

def render(results):
  if isinstance(results, Result):
    results.render()
  elif isinstance(results, list):
    for r in results:
      r.render()
  else:
    raise RuntimeError("Unknown result instance: %s", str(results.__class__))

# check if all results are successful
def isAllSuccessful(results):
  if isinstance(results, list):
    return all([result.is_successful() for result in results])
  elif isinstance(results, Result):
    return results.is_successful()
  else:
    raise RuntimeError("Unknown result instance: %s", str(results.__class__))
