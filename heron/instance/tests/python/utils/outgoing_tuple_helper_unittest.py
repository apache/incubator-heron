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

import heron.instance.tests.python.utils.mock_generator as mock_generator

class OutgoingTupleHelperTest(unittest.TestCase):
  DEFAULT_STREAM_ID = "stream_id"
  def setUp(self):
    pass

  def test_sample_success(self):
    out_helper = mock_generator.MockOutgoingTupleHelper()

    prim_data_tuple, size = mock_generator.make_data_tuple_from_list(mock_generator.prim_list)

    # Check adding data tuples and try sending out

    out_helper.add_data_tuple(self.DEFAULT_STREAM_ID, prim_data_tuple, size)
    # check if init_new_data_tuple() was properly called
    self.assertTrue(out_helper.called_init_new_data)
    self.assertEqual(out_helper.current_data_tuple_set.stream.id, self.DEFAULT_STREAM_ID)
    # check if it was properly added
    self.assertEqual(out_helper.current_data_tuple_size_in_bytes, size)
    # try sending out
    out_helper.send_out_tuples()
    self.assertEqual(out_helper.current_data_tuple_size_in_bytes, 0)
    self.assertEqual(out_helper.total_data_emitted_in_bytes, size)
    self.assertIsNone(out_helper.current_data_tuple_set)

    sent_data_tuple_set = out_helper.out_stream.poll().data
    self.assertEqual(sent_data_tuple_set.stream.id, self.DEFAULT_STREAM_ID)
    self.assertEqual(sent_data_tuple_set.tuples[0], prim_data_tuple)
