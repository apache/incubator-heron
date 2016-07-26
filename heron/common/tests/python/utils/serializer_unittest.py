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

# pylint: disable=missing-docstring

import unittest

from heron.common.src.python.utils.misc import PythonSerializer
import heron.common.tests.python.utils.mock_generator as mock_generator

class SerializerTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_py_serializer(self):
    serializer = PythonSerializer()
    serializer.initialize()
    # Test with a list of primitive types
    for obj in mock_generator.prim_list:
      serialized = serializer.serialize(obj)
      self.assertIsInstance(serialized, str)
      deserialized = serializer.deserialize(serialized)
      self.assertEqual(deserialized, obj)
