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


# pylint: disable=missing-docstring

import unittest
from unittest.mock import patch, Mock, call
from io import StringIO

from heron.common.src.python.utils import proc

class LogTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_stream_process_stdout(self):
    ret = StringIO(u'hello\nworld\n')
    log_fn = Mock()
    with patch("subprocess.Popen") as mock_process:
      mock_process.stdout = ret
      proc.stream_process_stdout(mock_process, log_fn)

    log_fn.assert_has_calls([call(u'hello\n'), call(u'world\n')])

  def test_async_stream_process_stdout(self):
    ret = StringIO(u'hello\nworld\n')
    log_fn = Mock()
    with patch("subprocess.Popen") as mock_process:
      mock_process.stdout = ret
      thread = proc.async_stream_process_stdout(mock_process, log_fn)
      thread.join()

    log_fn.assert_has_calls([call(u'hello\n'), call(u'world\n')])
