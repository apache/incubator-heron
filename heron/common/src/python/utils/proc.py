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

''' proc.py: subprocess and subprocess's stdout/stderr management '''
from threading import Thread

def _stream_process_fileno(fileno, handler):
  """ Stream handling each line from fileno
  :param filno: file object
  :param handler: a function that will be called for each line from fileno
  :return: None
  """
  while 1:
    line = fileno.readline()
    handler(line)
    if not line:
      break

def stream_process_stdout(process, handler):
  """ Stream the stdout for a process out to display
  :param process: the process to stream the stdout for
  :param handler: a function that will be called for each stdout line
  :return: None
  """
  _stream_process_fileno(process.stdout, handler)

def stream_process_stderr(process, handler):
  """ Stream the stderr for a process out to display
  :param process: the process to stream the stderr for
  :param handler: a function that will be called for each stderr line
  :return: None
  """
  _stream_process_fileno(process.stderr, handler)

def _async_stream_process_output(process, stream_fn, handler):
  """ Stream and handle the output of a process
  :param process: the process to stream the output for
  :param stream_fn: the function that applies handler to process
  :param handler: a function that will be called for each log line
  :return: None
  """
  logging_thread = Thread(target=stream_fn, args=(process, handler, ))

  # Setting the logging thread as a daemon thread will allow it to exit with the program
  # rather than blocking the exit waiting for it to be handled manually.
  logging_thread.daemon = True
  logging_thread.start()

  return logging_thread

def async_stream_process_stdout(process, handler):
  """ Stream and handler the stdout of a process
  :param process: the process to stream the stdout for
  :param handler: a function that will be called to handle each line
  :return: None
  """
  return _async_stream_process_output(process, stream_process_stdout, handler)

def async_stream_process_stderr(process, handler):
  """ Stream and handler the stderr of a process
  :param process: the process to stream the stderr for
  :param handler: a function that will be called to handle each line
  :return: None
  """
  return _async_stream_process_output(process, stream_process_stderr, handler)

class StringBuilder:
  def __init__(self):
    self.end = False
    self.strs = []

  def add(self, line: bytes):
    if not line:
      self.end = True
    else:
      self.strs.append(line)

  def result(self):
    while True:
      if self.end:
        return ''.join(self.strs)

def async_stdout_builder(proc):
  """ Save stdout into string builder
  :param proc: the process to save stdout for
  :return StringBuilder
  """
  stdout_builder = StringBuilder()
  async_stream_process_stdout(proc, stdout_builder.add)
  return stdout_builder

def async_stderr_builder(proc):
  """ Save stderr into string builder
  :param proc: the process to save stderr for
  :return StringBuilder
  """
  stderr_builder = StringBuilder()
  async_stream_process_stderr(proc, stderr_builder.add)
  return stderr_builder

def async_stdout_stderr_builder(proc):
  """ Save stdout and stderr into string builders
  :param proc: the process to save stdout and stderr for
  :return (StringBuilder, StringBuilder)
  """
  return async_stdout_builder(proc), async_stderr_builder(proc)
