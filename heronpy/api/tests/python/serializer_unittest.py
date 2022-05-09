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

from heronpy.api.serializer import PythonSerializer

prim_list = [1000, -234, 0.00023, "string",
             ["abc", "def", "ghi"], True, False,
             ("tuple", 123, True), None, {}]

class SerializerTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_py_serializer(self):
    serializer = PythonSerializer()
    serializer.initialize()
    # Test with a list of primitive types
    for obj in prim_list:
      serialized = serializer.serialize(obj)
      self.assertIsInstance(serialized, bytes)
      deserialized = serializer.deserialize(serialized)
      self.assertEqual(deserialized, obj)
