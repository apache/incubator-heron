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

'''Unittest for pex_loader'''
import os
import unittest
import re
import sys
import heron.common.src.python.pex_loader as pex_loader
import heron.common.tests.python.pex_loader.constants as constants

# pylint: disable=missing-docstring
class PexLoaderTest(unittest.TestCase):
  def test_deps_regex(self):
    # Testing egg_regex to find dependencies
    pass_test_cases = [".deps/sample_egg.egg/",
                       ".deps/sample_egg_1234.egg/",
                       ".deps/sample_egg.egg.egg/",
                       ".deps/sample_egg.whl/",
                       ".deps/sample.egg.whl/"]
    for test in pass_test_cases:
      # should match without the trailing slash
      self.assertEqual(re.match(pex_loader.egg_regex, test).group(1), test[:-1])

    fail_test_cases = [".deps/sample_egg/",
                       ".deps/sample_egg.egg",    # no trailing slash
                       ".deps/sample/egg.egg/",   # contains slash
                       ".deps/sample_ egg.egg/",  # contains space
                       "deps/sample_egg.egg/",    # not starting from .deps
                       "/.deps/sample_egg.egg/",  # starting from slash
                       ".deps/sample_whl/",
                       ".deps/sample.egg.wh/",
                       ".deps/sample.whl.egg"]
    for test in fail_test_cases:
      self.assertIsNone(re.match(pex_loader.egg_regex, test))

  def test_load_pex(self):
    # Testing load_pex without including deps (including deps requires an actual zip file)
    test_path = ['sample.pex', 'sample_123.pex', '/tmp/path.pex']
    for path in test_path:
      pex_loader.load_pex(path, include_deps=False)
      abs_path = os.path.abspath(path)
      self.assertIn(os.path.dirname(abs_path), sys.path)

  def test_sample(self):
    path = self.get_path_of_sample(constants.SAMPLE_PEX)
    print(path)
    pex_loader.load_pex(path)
    cls = pex_loader.import_and_get_class(path, constants.SAMPLE_PEX_CLASSPATH)
    self.assertIsNotNone(cls)
    self.assertEqual(cls.name, "sample class")
    self.assertEqual(cls.age, 100)

  @staticmethod
  def get_path_of_sample(sample):
    file_dir = "/".join(os.path.realpath(__file__).split('/')[:-1])
    testdata_dir = os.path.join(file_dir, constants.TEST_DATA_PATH)
    sample_pex_path = os.path.join(testdata_dir, sample)
    return sample_pex_path
