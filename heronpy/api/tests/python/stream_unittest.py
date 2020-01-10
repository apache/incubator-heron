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
# pylint: disable=protected-access
import unittest

from heronpy.api.stream import Stream, Grouping
from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.proto import topology_pb2

class DummyCustomGrouping(ICustomGrouping):
  def prepare(self, context, component, stream, target_tasks):
    pass
  def choose_tasks(self, values):
    pass

class StreamTest(unittest.TestCase):
  def test_default_stream_id(self):
    self.assertEqual(Stream.DEFAULT_STREAM_ID, "default")

  def test_constructor(self):
    # sane
    stream = Stream(fields=['word', 'count'])
    self.assertEqual(stream.fields, ['word', 'count'])
    self.assertEqual(stream.stream_id, "default")

    stream = Stream(fields=['error', 'message'], name='error_stream')
    self.assertEqual(stream.fields, ['error', 'message'])
    self.assertEqual(stream.stream_id, "error_stream")

    stream = Stream()
    self.assertEqual(stream.fields, [])
    self.assertEqual(stream.stream_id, "default")

    # fields not list, tuple nor None
    with self.assertRaises(TypeError):
      Stream(fields={"key": "value"})

    # fields contains non-string
    with self.assertRaises(TypeError):
      Stream(fields=["hello", 123, "world"])

    # stream name not string
    with self.assertRaises(TypeError):
      Stream(fields=["hello", "world"], name=True)
    with self.assertRaises(TypeError):
      Stream(fields=["hello", "world"], name=None)

class GroupingTest(unittest.TestCase):
  def test_is_grouping_sane(self):
    self.assertTrue(Grouping.is_grouping_sane(Grouping.ALL))
    self.assertTrue(Grouping.is_grouping_sane(Grouping.SHUFFLE))
    self.assertTrue(Grouping.is_grouping_sane(Grouping.LOWEST))
    self.assertTrue(Grouping.is_grouping_sane(Grouping.NONE))

    self.assertFalse(Grouping.is_grouping_sane(Grouping.FIELDS))
    sane_fields = Grouping.fields(['hello', 'world'])
    self.assertTrue(Grouping.is_grouping_sane(sane_fields))

    self.assertFalse(Grouping.is_grouping_sane(Grouping.CUSTOM))
    sane_custom = Grouping.custom(DummyCustomGrouping())
    self.assertTrue(Grouping.is_grouping_sane(sane_custom))

  def test_sparse_compatibility(self):
    self.assertEqual(Grouping.GLOBAL, Grouping.LOWEST)
    self.assertEqual(Grouping.LOCAL_OR_SHUFFLE, Grouping.SHUFFLE)

  def test_fields(self):
    # sane
    sane = Grouping.fields(['word', 'count'])
    self.assertEqual(sane.gtype, topology_pb2.Grouping.Value("FIELDS"))
    self.assertEqual(sane.fields, ['word', 'count'])

    sane = Grouping.fields("just_a_word")
    self.assertEqual(sane.gtype, topology_pb2.Grouping.Value("FIELDS"))
    self.assertEqual(sane.fields, ['just_a_word'])

    # non-string
    with self.assertRaises(TypeError):
      Grouping.fields(['word', 'count', True])
    with self.assertRaises(TypeError):
      Grouping.fields(123)
    with self.assertRaises(TypeError):
      Grouping.fields(None)

    # fields not specified
    with self.assertRaises(ValueError):
      Grouping.fields()

  def test_custom(self):
    # sane
    sane = Grouping.custom(DummyCustomGrouping())
    self.assertEqual(sane.gtype, topology_pb2.Grouping.Value("CUSTOM"))
    self.assertTrue(isinstance(sane.python_serialized, bytes))

    # arg not string
    with self.assertRaises(TypeError):
      Grouping.custom(None)
    with self.assertRaises(TypeError):
      Grouping.custom(True)
