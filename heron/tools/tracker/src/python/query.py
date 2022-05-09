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
"""
### <a name="metricsquery">Metrics Query Language</a>

Metrics queries are useful when some kind of aggregated values are required. For example,
to find the total number of tuples emitted by a spout, `SUM` operator can be used, instead
of fetching metrics for all the instances of the corresponding component, and then summing them.

#### Terminology

1. Univariate Timeseries --- A timeseries is called univariate if there is only one set
of minutely data. For example, a timeseries representing the sums of a number of timeseries
would be a univariate timeseries.
2. Multivariate Timeseries --- A set of multiple timeseries is collectively called multivariate.
Note that these timeseries are associated with their instances.

#### Operators

##### TS

```text
TS(componentName, instance, metricName)
```

Example:

```text
TS(component1, *, __emit-count/stream1)
```

Time Series Operator. This is the basic operator that is responsible for getting
metrics from TManager.
Accepts a list of 3 elements:

1. componentName
2. instance - can be "*" for all instances, or a single instance ID
3. metricName - Full metric name with stream id if applicable

Returns a univariate time series in case of a single instance id given, otherwise returns
a multivariate time series.

---

##### DEFAULT

```text
DEFAULT(0, TS(component1, *, __emit-count/stream1))
```
If the second operator returns more than one timeline, so will the
DEFAULT operator.

```text
DEFAULT(100.0, SUM(TS(component2, *, __emit-count/default))) <--
```
Second operator can be any operator

Default Operator. This operator is responsible for filling missing values in the metrics timeline.
Must have 2 arguments

1. First argument is a numeric constant representing the number to fill the missing values with
2. Second one must be one of the operators, that return the metrics timeline

Returns a univariate or multivariate time series, based on what the second operator is.

---

##### SUM

```text
SUM(TS(component1, instance1, metric1), DEFAULT(0, TS(component1, *, metric2)))
```

Sum Operator. This operator is used to take sum of all argument time series. It can have
any number of arguments,
each of which must be one of the following two types:

1. Numeric constants, which will fill in the missing values as well, or
2. Operator, which returns one or more timelines

Returns only a single timeline representing the sum of all time series for each timestamp.
Note that "instance" attribute is not there in the result.

---

##### MAX

```text
MAX(100, TS(component1, *, metric1))
```

Max Operator. This operator is used to find max of all argument operators for each
individual timestamp.
Each argument must be one of the following types:

1. Numeric constants, which will fill in the missing values as well, or
2. Operator, which returns one or more timelines

Returns only a single timeline representing the max of all the time series for each timestamp.
Note that "instance" attribute is not included in the result.

---

##### PERCENTILE

```text
PERCENTILE(99, TS(component1, *, metric1))
```

Percentile Operator. This operator is used to find a quantile of all timelines retuned by
the arguments, for each timestamp.
This is a more general type of query similar to MAX. Note that `PERCENTILE(100, TS...)` is
equivalent to `Max(TS...)`.
Each argument must be either constant or Operators.
First argument must always be the required Quantile.

1. Quantile (first argument) - Required quantile. 100 percentile = max, 0 percentile = min.
2. Numeric constants will fill in the missing values as well,
3. Operator - which returns one or more timelines

Returns only a single timeline representing the quantile of all the time series
for each timestamp. Note that "instance" attribute is not there in the result.

---

##### DIVIDE

```text
DIVIDE(TS(component1, *, metrics1), 100)
```

Divide Operator. Accepts two arguments, both can be univariate or multivariate.
Each can be of one of the following types:

1. Numeric constant will be considered as a constant time series for all applicable
  timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate
    1. Divide operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of division on the two
      corresponding time series.
2. When one operand is univariate, and other is multivariate
    1. This includes division by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate time series.
3. When both operands are univariate
    1. Instance information is ignored in this case
    2. Returns univariate time series which is the result of division operation.

---

##### MULTIPLY

```text
MULTIPLY(10, TS(component1, *, metrics1))
```

Multiply Operator. Has same conditions as division operator. This is to keep the API simple.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of
the following types:

1. Numeric constant will be considered as a constant time series for all applicable
  timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate
    1. Multiply operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of multiplication
        on the two corresponding time series.
2. When one operand is univariate, and other is multivariate
    1. This includes multiplication by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate timeseries.
3. When both operands are univariate
    1. Instance information is ignored in this case
    2. Returns univariate timeseries which is the result of multiplication operation.

---

##### SUBTRACT

```text
SUBTRACT(TS(component1, instance1, metrics1), TS(componet1, instance1, metrics2))

SUBTRACT(TS(component1, instance1, metrics1), 100)
```

Subtract Operator. Has same conditions as division operator. This is to keep the API simple.
Accepts two arguments, both can be univariate or multivariate. Each can be of one of
the following types:

1. Numeric constant will be considered as a constant time series for all applicable
  timestamps, they will not fill the missing values
2. Operator - returns one or more timelines

Three main cases are:

1. When both operands are multivariate
    1. Subtract operation will be done on matching data, that is, with same instance id.
    2. If the instances in both the operands do not match, error is thrown.
    3. Returns multivariate time series, each representing the result of subtraction
        on the two corresponding time series.
2. When one operand is univariate, and other is multivariate
    1. This includes subtraction by constants as well.
    2. The univariate operand will participate with all time series in multivariate.
    3. The instance information of the multivariate time series will be preserved in the result.
    4. Returns multivariate time series.
3. When both operands are univariate
    1. Instance information is ignored in this case
    2. Returns univariate time series which is the result of subtraction operation.

---

##### RATE

```text
RATE(SUM(TS(component1, *, metrics1)))

RATE(TS(component1, *, metrics2))
```

Rate Operator. This operator is used to find rate of change for all timeseries.
Accepts a only a single argument, which must be an Operators which returns
univariate or multivariate time series.
Returns univariate or multivariate time series based on the argument, with each
timestamp value corresponding to the rate of change for that timestamp.
"""

from typing import Any, List, Optional

from heron.proto.tmanager_pb2 import TManagerLocation
from heron.tools.tracker.src.python.query_operators import *


####################################################################
# Parsing and executing the query string.
####################################################################

# pylint: disable=no-self-use
class Query:
  """Execute the query for metrics. Uses Tracker to get
     individual metrics that are part of the query.
     Example usage:
        query = Query(tracker)
        result = query.execute(tmanager, query_string)"""
  # pylint: disable=undefined-variable
  def __init__(self, tracker):
    self.tracker = tracker
    self.operators = {
        'TS':TS,
        'DEFAULT':Default,
        'MAX':Max,
        'SUM':Sum,
        'SUBTRACT':Subtract,
        'PERCENTILE':Percentile,
        'DIVIDE':Divide,
        'MULTIPLY':Multiply,
        'RATE':Rate
    }


  # pylint: disable=attribute-defined-outside-init, no-member
  async def execute_query(
      self,
      tmanager: TManagerLocation,
      query_string: str,
      start: int,
      end: int
  ) -> Any:
    """ execute query """
    if not tmanager:
      raise Exception("No tmanager found")
    self.tmanager = tmanager
    root = self.parse_query_string(query_string)
    return await root.execute(self.tracker, self.tmanager, start, end)

  def find_closing_braces(self, query: str) -> int:
    """Find the index of the closing braces for the opening braces
    at the start of the query string. Note that first character
    of input string must be an opening braces."""
    if query[0] != '(':
      raise Exception("Trying to find closing braces for no opening braces")
    num_open_braces = 0
    for i, c in enumerate(query):
      if c == '(':
        num_open_braces += 1
      elif c == ')':
        num_open_braces -= 1
      if num_open_braces == 0:
        return i
    raise Exception("No closing braces found")

  def get_sub_parts(self, query: str) -> List[str]:
    """The subparts are seperated by a comma. Make sure
    that commas inside the part themselves are not considered."""
    parts: List[str] = []
    num_open_braces = 0
    delimiter = ','
    last_starting_index = 0
    for i, c in enumerate(query):
      if c == '(':
        num_open_braces += 1
      elif c == ')':
        num_open_braces -= 1
        if num_open_braces < 0:
          raise Exception("Too many closing braces")
      elif c == delimiter and num_open_braces == 0:
        parts.append(query[last_starting_index: i].strip())
        last_starting_index = i + 1
    parts.append(query[last_starting_index:].strip())
    return parts

  def parse_query_string(self, query: str) -> Optional[Operator]:
    """Returns a parse tree for the query, each of the node is a
    subclass of Operator. This is both a lexical as well as syntax analyzer step."""
    query = query.strip()
    if not query:
      return None
    # Just braces do not matter
    if query[0] == '(':
      index = self.find_closing_braces(query)
      # This must be the last index, since this was an NOP starting brace
      if index != len(query) - 1:
        raise Exception("Invalid syntax")
      return self.parse_query_string(query[1:index])
    start_index = query.find("(")
    # There must be a ( in the query
    if start_index < 0:
      # Otherwise it must be a constant
      try:
        constant = float(query)
        return constant
      except ValueError:
        raise Exception("Invalid syntax") from ValueError
    token = query[:start_index].rstrip()
    operator_cls = self.operators.get(token)
    if operator_cls is None:
      raise Exception(f"Invalid token: {token!r}")

    # Get sub components
    rest_of_the_query = query[start_index:]
    braces_end_index = self.find_closing_braces(rest_of_the_query)
    if braces_end_index != len(rest_of_the_query) - 1:
      raise Exception("Invalid syntax")
    parts = self.get_sub_parts(rest_of_the_query[1:-1])

    # parts are simple strings in this case
    if token == "TS":
      # This will raise exception if parts are not syntactically correct
      return operator_cls(parts)

    children = [
        self.parse_query_string(part)
        for part in parts
    ]

    # Make a node for the current token
    node = operator_cls(children)
    return node
