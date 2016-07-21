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

import unittest

import heron.instance.tests.python.mock_generator as mock_generator

class BoltTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_simple_read_and_emit(self):
    # in_stream == out_stream in this case
    # send and receive a single primitive tuple
    bolt = mock_generator.MockBolt()
    bolt.emit(mock_generator.prim_list)
    bolt.output_helper.send_out_tuples()
    bolt._read_tuples_and_execute()
    self.assertIsNotNone(bolt.received_data_tuple)






