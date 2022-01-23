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
from unittest.mock import MagicMock

from heron.tools.tracker.src.python.query import *
from heron.tools.tracker.src.python.tracker import Tracker

import pytest

@pytest.fixture
def mock_query():
  tracker = MagicMock(Tracker)
  return Query(tracker)

def test_find_closing_braces(mock_query):
  query = "(())"
  assert 3 == mock_query.find_closing_braces(query)

  query = "hello()"
  with pytest.raises(Exception):
    mock_query.find_closing_braces(query)

  query = "(hello)"
  assert 6 == mock_query.find_closing_braces(query)

  query = "(no closing braces"
  with pytest.raises(Exception):
    mock_query.find_closing_braces(query)

  query = "()()"
  assert 1 == mock_query.find_closing_braces(query)

def test_get_sub_parts(mock_query):
  query = "abc, def, xyz"
  assert ["abc", "def", "xyz"] == mock_query.get_sub_parts(query)

  query = "(abc, xyz)"
  assert ["(abc, xyz)"] == mock_query.get_sub_parts(query)

  query = "a(x, y), b(p, q)"
  assert ["a(x, y)", "b(p, q)"] == mock_query.get_sub_parts(query)

  query = ",,"
  assert ["", "", ""] == mock_query.get_sub_parts(query)

  query = "())"
  with pytest.raises(Exception):
    mock_query.get_sub_parts(query)

  # pylint: disable=too-many-statements
def test_parse_query_string(mock_query):
  query = "TS(a, b, c)"
  root = mock_query.parse_query_string(query)
  assert "a" == root.component
  assert ["b"] == root.instances
  assert "c" == root.metric_name

  query = "TS(a, *, m)"
  root = mock_query.parse_query_string(query)
  assert "a" == root.component
  assert [] == root.instances
  assert "m" == root.metric_name

  query = "DEFAULT(0, TS(a, b, c))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Default)
  assert isinstance(root.constant, float)
  assert root.constant == 0
  assert isinstance(root.timeseries, TS)

  query = "DEFAULT(0, SUM(TS(a, a, a), TS(b, b, b)))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Default)
  assert isinstance(root.constant, float)
  assert isinstance(root.timeseries, Sum)
  assert 2 == len(root.timeseries.time_series_list)
  assert isinstance(root.timeseries.time_series_list[0], TS)
  assert isinstance(root.timeseries.time_series_list[1], TS)

  query = "MAX(1, TS(a, a, a))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Max)
  assert isinstance(root.time_series_list[0], float)
  assert isinstance(root.time_series_list[1], TS)

  query = "PERCENTILE(90, TS(a, a, a))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Percentile)
  assert isinstance(root.quantile, float)
  assert isinstance(root.time_series_list[0], TS)

  query = "PERCENTILE(TS(a, a, a), 90)"
  with pytest.raises(Exception):
    mock_query.parse_query_string(query)

  query = "DIVIDE(TS(a, a, a), TS(b, b, b))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Divide)
  assert isinstance(root.operand1, TS)
  assert isinstance(root.operand2, TS)

    # Dividing by a constant is fine
  query = "DIVIDE(TS(a, a, a), 90)"
  mock_query.parse_query_string(query)
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Divide)
  assert isinstance(root.operand1, TS)
  assert isinstance(root.operand2, float)

    # Must have two operands
  query = "DIVIDE(TS(a, a, a))"
  with pytest.raises(Exception):
    mock_query.parse_query_string(query)

  query = "MULTIPLY(TS(a, a, a), TS(b, b, b))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Multiply)
  assert isinstance(root.operand1, TS)
  assert isinstance(root.operand2, TS)

    # Multiplying with a constant is fine.
  query = "MULTIPLY(TS(a, a, a), 10)"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Multiply)
  assert isinstance(root.operand1, TS)
  assert isinstance(root.operand2, float)

    # Must have two operands
  query = "MULTIPLY(TS(a, a, a))"
  with pytest.raises(Exception):
    mock_query.parse_query_string(query)

  query = "SUBTRACT(TS(a, a, a), TS(b, b, b))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Subtract)
  assert isinstance(root.operand1, TS)
  assert isinstance(root.operand2, TS)

    # Multiplying with a constant is fine.
  query = "SUBTRACT(TS(a, a, a), 10)"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Subtract)
  assert isinstance(root.operand1, TS)
  assert isinstance(root.operand2, float)

    # Must have two operands
  query = "SUBTRACT(TS(a, a, a))"
  with pytest.raises(Exception):
    mock_query.parse_query_string(query)

  query = "RATE(TS(a, a, a))"
  root = mock_query.parse_query_string(query)
  assert isinstance(root, Rate)
  assert isinstance(root.time_series, TS)

    # Must have one operand only
  query = "RATE(TS(a, a, a), TS(b, b, b))"
  with pytest.raises(Exception):
    mock_query.parse_query_string(query)
