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

import time
from heron.instance.src.python.utils.tuple import TupleHelper
import heron.instance.tests.python.mock_protobuf as mock_protobuf
import heron.instance.tests.python.utils.mock_generator as mock_generator

class TupleHelperTest(unittest.TestCase):
  def test_normal_tuple(self):
    STREAM = mock_protobuf.get_mock_stream_id(id="stream_id", component_name="comp_name")
    TUPLE_KEY = "tuple_key"
    VALUES = mock_generator.prim_list

    # No roots
    tup = TupleHelper.make_tuple(STREAM, TUPLE_KEY, VALUES)
    self.assertEqual(tup.id, TUPLE_KEY)
    self.assertEqual(tup.component, STREAM.component_name)
    self.assertEqual(tup.stream, STREAM.id)
    self.assertIsNone(tup.task)
    self.assertEqual(tup.values, VALUES)
    self.assertAlmostEqual(tup.creation_time, time.time(), delta=0.01)
    self.assertIsNone(tup.roots)

  def test_tick_tuple(self):
    tup = TupleHelper.make_tick_tuple()
    self.assertEqual(tup.id, "__tick")
    self.assertEqual(tup.component, "__system")
    self.assertEqual(tup.stream, "__tick")
    self.assertIsNone(tup.task)
    self.assertIsNone(tup.values)
    self.assertIsNone(tup.roots)
    self.assertAlmostEqual(tup.creation_time, time.time(), delta=0.01)

  def test_root_tuple_info(self):
    STREAM_ID = "stream id"
    TUPLE_ID = "tuple_id"

    root_info = TupleHelper.make_root_tuple_info(STREAM_ID, TUPLE_ID)
    self.assertEqual(root_info.stream_id, STREAM_ID)
    self.assertEqual(root_info.tuple_id, TUPLE_ID)
