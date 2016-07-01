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
import Queue
import unittest

from heron.instance.src.python.misc.communicator import HeronCommunicator
import heron.instance.tests.python.mock_generator as mock_generator

class CommunicatorTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_generic(self):
    """Unittest with no producer, consumer specified"""
    communicator = HeronCommunicator(producer=None, consumer=None)
    for obj in mock_generator.prim_list:
      communicator.offer(obj)

    for obj in mock_generator.prim_list:
      self.assertEqual(obj, communicator.poll())


  def test_empty(self):
    communicator = HeronCommunicator(producer=None, consumer=None)
    with self.assertRaises(Queue.Empty):
      communicator.poll()

