#!/usr/bin/env python3
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

'''result.py'''
import abc
import sys
from enum import Enum

from heron.common.src.python.utils import proc
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

# Definition corresponds to definition in org.apache.heron.scheduler.AbstractMain

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
  if status_code < 100:
    return Status.InvocationError
  if status_code == 200:
    return Status.DryRun
  return Status.HeronError

class Result:
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
      print(msg, file=f)

  def _log_context(self):
    # render context only after process exits
    assert self.status is not None
    if self.status in [Status.Ok, Status.DryRun]:
      self._do_log(Log.info, self.succ_context)
    elif self.status in [Status.HeronError, Status.InvocationError]:
      self._do_log(Log.error, self.err_context)
    else:
      raise RuntimeError(
          f"Unknown status type of value {self.status.value}. Expected value: {list(Status)}"
        )

  def add_context(self, err_context, succ_context=None):
    """ Prepend msg to add some context information

    :param pmsg: context info
    :return: None
    """
    self.err_context = err_context
    self.succ_context = succ_context

  @abc.abstractmethod
  def render(self):
    pass


class SimpleResult(Result):
  """Simple result: result that already and only
     contains status of the result"""
  def __init__(self, *args):
    super().__init__(*args)

  def render(self):
    self._log_context()


class ProcessResult(Result):
  """Process result: a wrapper of result class"""
  def __init__(self, process):
    super().__init__()
    self.process = process
    self.stdout_builder = proc.async_stdout_builder(process)
    # start redirect stderr in initialization, before render() gets called
    proc.async_stream_process_stderr(self.process, self.renderProcessStdErr)

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
    retcode = self.process.poll()
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
      raise RuntimeError("Unknown status type of value "\
        f"{self.status.value}. Expected value: {list(Status)}")

  def render(self):
    self.process.wait()
    self.status = status_type(self.process.returncode)
    self.renderProcessStdOut(self.stdout_builder.result())
    self._log_context()

def render(results):
  if isinstance(results, Result):
    results.render()
  elif isinstance(results, list):
    for r in results:
      r.render()
  else:
    raise RuntimeError(f"Unknown result instance: {(str(results.__class__),)}")

# check if all results are successful
def is_successful(results):
  if isinstance(results, list):
    # pylint: disable=use-a-generator
    return all([is_successful(result) for result in results])
  if isinstance(results, Result):
    return results.status in (Status.Ok, Status.DryRun)
  raise RuntimeError(f"Unknown result instance: {(str(results.__class__),)}")
