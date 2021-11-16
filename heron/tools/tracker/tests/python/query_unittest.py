# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
''' query_unittest.py '''
# pylint: disable=missing-docstring, undefined-variable
import unittest
from unittest.mock import Mock

from heron.tools.tracker.src.python.query import *

class QueryTest(unittest.TestCase):
  def setUp(self):
    self.tracker = Mock()
    self.query = Query(self.tracker)

  def test_find_closing_braces(self):
    query = "(())"
    self.assertEqual(3, self.query.find_closing_braces(query))

    query = "hello()"
    with self.assertRaises(Exception):
      self.query.find_closing_braces(query)

    query = "(hello)"
    self.assertEqual(6, self.query.find_closing_braces(query))

    query = "(no closing braces"
    with self.assertRaises(Exception):
      self.query.find_closing_braces(query)

    query = "()()"
    self.assertEqual(1, self.query.find_closing_braces(query))

  def test_get_sub_parts(self):
    query = "abc, def, xyz"
    self.assertEqual(["abc", "def", "xyz"], self.query.get_sub_parts(query))

    query = "(abc, xyz)"
    self.assertEqual(["(abc, xyz)"], self.query.get_sub_parts(query))

    query = "a(x, y), b(p, q)"
    self.assertEqual(["a(x, y)", "b(p, q)"], self.query.get_sub_parts(query))

    query = ",,"
    self.assertEqual(["", "", ""], self.query.get_sub_parts(query))

    query = "())"
    with self.assertRaises(Exception):
      self.query.get_sub_parts(query)

  # pylint: disable=too-many-statements
  def test_parse_query_string(self):
    query = "TS(a, b, c)"
    root = self.query.parse_query_string(query)
    self.assertEqual("a", root.component)
    self.assertEqual(["b"], root.instances)
    self.assertEqual("c", root.metric_name)

    query = "TS(a, *, m)"
    root = self.query.parse_query_string(query)
    self.assertEqual("a", root.component)
    self.assertEqual([], root.instances)
    self.assertEqual("m", root.metric_name)

    query = "DEFAULT(0, TS(a, b, c))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Default)
    self.assertIsInstance(root.constant, float)
    self.assertEqual(root.constant, 0)
    self.assertIsInstance(root.timeseries, TS)

    query = "DEFAULT(0, SUM(TS(a, a, a), TS(b, b, b)))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Default)
    self.assertIsInstance(root.constant, float)
    self.assertIsInstance(root.timeseries, Sum)
    self.assertEqual(2, len(root.timeseries.timeSeriesList))
    self.assertIsInstance(root.timeseries.timeSeriesList[0], TS)
    self.assertIsInstance(root.timeseries.timeSeriesList[1], TS)

    query = "MAX(1, TS(a, a, a))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Max)
    self.assertIsInstance(root.timeSeriesList[0], float)
    self.assertIsInstance(root.timeSeriesList[1], TS)

    query = "PERCENTILE(90, TS(a, a, a))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Percentile)
    self.assertIsInstance(root.quantile, float)
    self.assertIsInstance(root.timeSeriesList[0], TS)

    query = "PERCENTILE(TS(a, a, a), 90)"
    with self.assertRaises(Exception):
      self.query.parse_query_string(query)

    query = "DIVIDE(TS(a, a, a), TS(b, b, b))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Divide)
    self.assertIsInstance(root.operand1, TS)
    self.assertIsInstance(root.operand2, TS)

    # Dividing by a constant is fine
    query = "DIVIDE(TS(a, a, a), 90)"
    self.query.parse_query_string(query)
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Divide)
    self.assertIsInstance(root.operand1, TS)
    self.assertIsInstance(root.operand2, float)

    # Must have two operands
    query = "DIVIDE(TS(a, a, a))"
    with self.assertRaises(Exception):
      self.query.parse_query_string(query)

    query = "MULTIPLY(TS(a, a, a), TS(b, b, b))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Multiply)
    self.assertIsInstance(root.operand1, TS)
    self.assertIsInstance(root.operand2, TS)

    # Multiplying with a constant is fine.
    query = "MULTIPLY(TS(a, a, a), 10)"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Multiply)
    self.assertIsInstance(root.operand1, TS)
    self.assertIsInstance(root.operand2, float)

    # Must have two operands
    query = "MULTIPLY(TS(a, a, a))"
    with self.assertRaises(Exception):
      self.query.parse_query_string(query)

    query = "SUBTRACT(TS(a, a, a), TS(b, b, b))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Subtract)
    self.assertIsInstance(root.operand1, TS)
    self.assertIsInstance(root.operand2, TS)

    # Multiplying with a constant is fine.
    query = "SUBTRACT(TS(a, a, a), 10)"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Subtract)
    self.assertIsInstance(root.operand1, TS)
    self.assertIsInstance(root.operand2, float)

    # Must have two operands
    query = "SUBTRACT(TS(a, a, a))"
    with self.assertRaises(Exception):
      self.query.parse_query_string(query)

    query = "RATE(TS(a, a, a))"
    root = self.query.parse_query_string(query)
    self.assertIsInstance(root, Rate)
    self.assertIsInstance(root.timeSeries, TS)

    # Must have one operand only
    query = "RATE(TS(a, a, a), TS(b, b, b))"
    with self.assertRaises(Exception):
      self.query.parse_query_string(query)
