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
      if msg[-1] == '\n':
        msg = msg[:-1]
      log_f(msg)

  @staticmethod
  def _do_print(f, msg):
    if msg:
      if msg[-1] == '\n':
        msg = msg[:-1]
      print >> f, msg

  def _log_context(self):
    # render context only after process exits
    assert self.status is not None
    if self.status in [Status.Ok, Status.DryRun]:
      self._do_log(Log.info, self.succ_context)
    elif self.status in [Status.HeronError, Status.InvocationError]:
      self._do_log(Log.error, self.err_context)
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
  def __init__(self, *args):
    super(SimpleResult, self).__init__(*args)

  def render(self):
    self._log_context()


class ProcessResult(Result):
  """Process result: a wrapper of result class"""
  def __init__(self, proc):
    super(ProcessResult, self).__init__()
    self.proc = proc

  def renderProcessStdErr(self, stderr_line):
    """ render stderr of shelled-out process
        stderr could be error message of failure of invoking process or
        normal stderr output from successfully shelled-out process.
        In the first case, ``Popen'' should fail fast and we should be able to
        get return code immediately. We then render the failure message.
        In the second case, we simply print stderr line in stderr.
        The way to handle the first case is shaky but should be the best we can
        do since we have conflicts of design goals here.
    :param stderr_line: one line from shelled-out process
    :return:
    """
    retcode = self.proc.poll()
    if retcode is not None and status_type(retcode) == Status.InvocationError:
      self._do_log(Log.error, stderr_line)
    else:
      self._do_print(sys.stderr, stderr_line)

  def renderProcessStdOut(self, stdout):
    """ render stdout of shelled-out process
        stdout always contains information Java process wants to
        propagate back to cli, so we do special rendering here
    :param stdout: all lines from shelled-out process
    :return:
    """
    # since we render stdout line based on Java process return code,
    # ``status'' has to be already set
    assert self.status is not None
    # remove pending newline
    if self.status == Status.Ok:
      self._do_log(Log.info, stdout)
    elif self.status == Status.HeronError:
      # remove last newline since logging will append newline
      self._do_log(Log.error, stdout)
    # No need to prefix [INFO] here. We want to display dry-run response in a clean way
    elif self.status == Status.DryRun:
      self._do_print(sys.stdout, stdout)
    elif self.status == Status.InvocationError:
      self._do_print(sys.stdout, stdout)
    else:
      raise RuntimeError(
          "Unknown status type of value %d. Expected value: %s" % \
          (self.status.value, list(Status)))

  def render(self):
    while True:
      stderr_line = self.proc.stderr.readline()
      if not stderr_line:
        if self.proc.poll() is None:
          continue
        else:
          break
      else:
        self.renderProcessStdErr(stderr_line)
    self.proc.wait()
    self.status = status_type(self.proc.returncode)
    stdout = "".join(self.proc.stdout.readlines())
    self.renderProcessStdOut(stdout)
    self._log_context()


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
