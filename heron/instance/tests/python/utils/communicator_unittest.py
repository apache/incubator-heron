#!/usr/bin/env python
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
from queue import Empty
import unittest

from heron.instance.src.python.utils.misc import HeronCommunicator
import heron.instance.tests.python.utils.mock_generator as mock_generator

class CommunicatorTest(unittest.TestCase):
  def setUp(self):
    self.global_value = 6

  def test_generic(self):
    """Unittest with no producer, consumer specified"""
    communicator = HeronCommunicator(producer_cb=None, consumer_cb=None)
    for obj in mock_generator.prim_list:
      communicator.offer(obj)

    for obj in mock_generator.prim_list:
      self.assertEqual(obj, communicator.poll())

  def test_empty(self):
    communicator = HeronCommunicator(producer_cb=None, consumer_cb=None)
    with self.assertRaises(Empty):
      communicator.poll()

  def test_producer_callback(self):
    def callback():
      self.global_value = 10

    # test producer cb
    communicator = HeronCommunicator(producer_cb=callback, consumer_cb=None)
    communicator.offer("object")
    self.assertEqual(self.global_value, 6)
    ret = communicator.poll()
    self.assertEqual(ret, "object")
    self.assertEqual(self.global_value, 10)

  def test_consumer_callback(self):
    def callback():
      self.global_value = 10

    # test consumer cb
    communicator = HeronCommunicator(producer_cb=None, consumer_cb=callback)
    self.assertEqual(self.global_value, 6)
    communicator.offer("object")
    self.assertEqual(self.global_value, 10)
