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

''' opts_unittest.py '''
import unittest
import heron.tools.cli.src.python.opts as opts

#pylint: disable=missing-docstring, no-self-use

class OptsTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_one_opt(self):
    opts.clear_config()
    opts.set_config('XX', 'xx')
    self.assertEqual('xx', opts.get_config('XX'))

  def test_two_opts(self):
    opts.clear_config()
    opts.set_config('XX', 'xx')
    opts.set_config('YY', 'yy')
    self.assertEqual('xx', opts.get_config('XX'))
    self.assertEqual('yy', opts.get_config('YY'))

  def test_non_exist_key(self):
    opts.clear_config()
    opts.set_config('XX', 'xx')
    self.assertEqual(None, opts.get_config('YY'))

  def test_many_opts(self):
    opts.clear_config()
    for k in range(1, 100):
      key = f"key-{k}"
      value = f"value-{k}"
      opts.set_config(key, value)

    for k in range(1, 100):
      key = f"key-{k}"
      value = f"value-{k}"
      self.assertEqual(value, opts.get_config(key))

  def test_clear_opts(self):
    opts.clear_config()
    opts.set_config('YY', 'yy')
    self.assertEqual('yy', opts.get_config('YY'))
    opts.clear_config()
    self.assertEqual(None, opts.get_config('YY'))

  def tearDown(self):
    opts.clear_config()
