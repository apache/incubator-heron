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

''' query_operators.py '''
import asyncio
import math

from typing import Any, Dict, List, Optional, Union

from heron.proto.tmanager_pb2 import TManagerLocation
from heron.tools.tracker.src.python.metricstimeline import get_metrics_timeline


#####################################################################
# Data Structure for fetched Metrics
#####################################################################
class Metrics:
  """Represents a univariate timeseries.
  Multivariate timeseries is simply a list of this."""
  __slots__ = ["component_name", "metric_name", "instance", "start", "end", "timeline"]

  def __init__(
      self,
      component_name: str,
      metric_name: str,
      instance,
      start: int,
      end: int,
      timeline: Dict[int, float],
  ):
    """Insantiate class with a floored copy of the timeline within [start, end]."""
    self.component_name = component_name
    self.metric_name = metric_name
    self.instance = instance
    self.start = start
    self.end = end
    self.timeline = self.floorTimestamps(start, end, timeline)

  @staticmethod
  def floorTimestamps(
      start: int,
      end: int,
      timeline: Dict[int, float],
  ) -> Dict[int, float]:
    """
    Return a copy of the timeline with timestamps in [start, end],
    floored down to the minute.

    If multiple timestamps floor to same timestamp, only the value of the last
    one enumerated will be used.

    """
    return {
        timestamp // 60 * 60: value
        for timestamp, value in timeline.items()
        if start <= timestamp // 60 * 60 <= end
    }

  def setDefault(self, constant: int, start: int, end: int) -> None:
    """
    Pad the timeline with the constant for any floored intervals in [start, end].

    Timestamps should be floored for expected behaviour.

    """
    starttime = math.ceil(start / 60) * 60
    endtime = end // 60 * 60
    while starttime <= endtime:
      # STREAMCOMP-1559
      # Second check is a work around, because the response from tmanager
      # contains value 0, if it is queries for the current timestamp,
      # since the bucket is created in the tmanager, but is not filled
      # by the metrics.
      if not self.timeline.get(starttime):
        self.timeline[starttime] = constant
      starttime += 60

################################################################
# All the Operators supported by query system.
################################################################

# pylint: disable=no-self-use
class Operator:
  """Base class for all operators"""
  def __init__(self, _):
    raise Exception("Not implemented exception")

  # pylint: disable=unused-argument
  async def execute(self, tracker, tmanager: TManagerLocation, start: int, end: int) -> Any:
    """ execute """
    raise Exception("Not implemented exception")


class TS(Operator):
  """
  Time Series Operator. This is the basic operator that is
  responsible for getting metrics from tmanager.
  Accepts a list of 3 elements:
  1. component_name
  2. instance - can be "*" for all instances, or a single instance ID
  3. metric_name - Full metric name with stream id if applicable
  Returns a list of Metrics objects, each representing single timeseries

  """
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 3:
      raise Exception("TS requires 3 arguments")
    self.component, instance, self.metric_name = children

    # A '*' represents all instances, which is represented by empty array.
    # Otherwise, it represents a single instance
    self.instances = []
    if instance != "*":
      self.instances.append(instance)

  async def execute(
      self,
      tracker,
      tmanager: TManagerLocation,
      start: int,
      end: int,
    ) -> Optional[Any]:
    # Fetch metrics for start-60 to end+60 because the minute mark
    # may be a little skewed. By getting a couple more values,
    # we can then truncate based on the interval needed.
    metrics = await get_metrics_timeline(
        tmanager, self.component, [self.metric_name], self.instances,
        start - 60, end + 60)
    if not metrics:
      return
    if "message" in metrics:
      raise Exception(metrics["message"])

    # Put a blank timeline.
    if not metrics.timeline:
      metrics.timeline = {
          self.metric_name: {}
      }
    timelines = metrics.timeline[self.metric_name]
    all_metrics = [
        Metrics(self.component, self.metric_name, instance, start, end, {
            k: float(v)
            for k, v in timeline.items()
            if not math.isnan(float(v))
        })
        for instance, timeline in timelines.items()
    ]
    return all_metrics


class Default(Operator):
  """
  Default Operator. This operator is responsible for filling
  holes in the metrics timeline of its children.
  Accepts a list of 2 elements:
  1. constant to fill the holes with
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of Metrics objects, each representing single timeseries

  """
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 2:
      raise Exception("DEFAULT requires 2 arguments")
    default, timeseries = children
    if not isinstance(default, float):
      raise Exception("First argument to DEFAULT must be a number")
    if not isinstance(timeseries, Operator):
      raise Exception(
          f"Second argument to DEFAULT must be an operator, but is {type(timeseries)}"
      )
    self.constant = default
    self.timeseries = timeseries

  async def execute(self, tracker, tmanager: TManagerLocation, start: int, end: int) -> Any:
    all_metrics = await self.timeseries.execute(tracker, tmanager, start, end)
    if isinstance(all_metrics, str):
      raise Exception(all_metrics)
    for metric in all_metrics:
      metric.setDefault(self.constant, start, end)
    return all_metrics

class Sum(Operator):
  """
  Sum Operator. This operator is used to take sum of all children timeseries.
  Accepts a list of elements, all of which have to be either constant or Operators.
  Note that the length of the children is unbounded.
  1. constants will fill in the holes as well, if present in other timeseries
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of only one Metrics object, representing sum of all timeseries

  """
  # pylint: disable=super-init-not-called
  def __init__(self, children) -> None:
    self.time_series_list = children

  async def execute(self, tracker, tmanager: TManagerLocation, start: int, end: int) -> Any:
    # Initialize the metric to be returned with sum of all the constants.
    result = Metrics(None, None, None, start, end, {})
    constant_sum = sum(ts for ts in self.time_series_list if isinstance(ts, float))
    result.setDefault(constant_sum, start, end)

    futureMetrics = [
        ts.execute(tracker, tmanager, start, end)
        for ts in self.time_series_list if isinstance(ts, Operator)
    ]

    # Get all the timeseries metrics
    all_metrics = []
    for met_f in asyncio.as_completed(futureMetrics):
      met = await met_f
      if isinstance(met, str):
        raise Exception(met)
      all_metrics.extend(met)

    # Aggregate all of the them
    for metric in all_metrics:
      for timestamp, value in metric.timeline.items():
        # Metrics could use a Counter instead of dict and just do
        # result.timeline.update(timeline) which uses C
        if timestamp in result.timeline:
          result.timeline[timestamp] += value
    return [result]

class Max(Operator):
  """Max Operator. This operator is used to find max of all children timeseries
  for each individual timestamp.
  Accepts a list of elements, all of which have to be either constant or Operators.
  Note that the length of the children is unbounded.
  1. constants will fill in the holes as well, if present in other timeseries
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of only one Metrics object, representing max of all timeseries"""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) < 1:
      raise Exception("MAX expects at least one operand.")
    self.time_series_list = children

  async def execute(self, tracker, tmanager: TManagerLocation, start: int, end: int) -> Any:
    # Initialize the metric to be returned with max of all the constants.
    result = Metrics(None, None, None, start, end, {})
    constants = [ts for ts in self.time_series_list if isinstance(ts, float)]
    if constants:
      result.setDefault(max(constants), start, end)

    futureMetrics = [
        ts.execute(tracker, tmanager, start, end)
        for ts in self.time_series_list if isinstance(ts, Operator)
    ]

    # Get all the timeseries metrics
    all_metrics = []
    for met_f in asyncio.as_completed(futureMetrics):
      met = await met_f
      if isinstance(met, str):
        raise Exception(met)
      all_metrics.extend(met)

    # Aggregate all of the them
    for metric in all_metrics:
      for timestamp, value in metric.timeline.items():
        if start <= timestamp <= end:
          if timestamp not in result.timeline:
            result.timeline[timestamp] = value
          result.timeline[timestamp] = max(value, result.timeline[timestamp])
    return [result]

class Percentile(Operator):
  """
  Percentile Operator. This operator is used to find a quantile of all children
  timeseries for each individual timestamp. This is a more general type of query
  than max. Percentile(100, TS...) is equivalent to Max(TS...).
  Accepts a list of elements, all of which have to be either constant or Operators.
  Note that the length of the children is unbounded.
  First argument must always be the required Quantile.
  1. Quantile - Required quantile. 100 percentile = max, 0 percentile = min.
  2. constants will fill in the holes as well, if present in other timeseries
  3. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of only one Metrics object, representing quantile of all timeseries

  """

  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) < 1:
      raise Exception("PERCENTILE expects at least two operands.")
    quantile, *timeseries_list = children
    if not isinstance(quantile, float):
      raise Exception("First argument to PERCENTILE must be a constant")
    if not 0 <= quantile <= 100:
      raise Exception("Quantile must be between 0 and 100 inclusive.")
    self.quantile = quantile
    self.time_series_list = timeseries_list

  async def execute(self, tracker, tmanager, start, end):
    futureMetrics = [
        ts.execute(tracker, tmanager, start, end)
        for ts in self.time_series_list if isinstance(ts, Operator)
    ]

    # Get all the timeseries metrics
    all_metrics = []
    for met_f in asyncio.as_completed(futureMetrics):
      met = await met_f
      if isinstance(met, str):
        raise Exception(met)
      all_metrics.extend(met)

    # accumulate all the values for a timestamp and we will later do
    # a percentile on it
    listed_timeline: Dict[int, List[float]] = {}
    for metric in all_metrics:
      for timestamp, value in metric.timeline.items():
        if start <= timestamp <= end:
          if timestamp not in listed_timeline:
            listed_timeline[timestamp] = []
          listed_timeline[timestamp].append(value)

    timeline = {}
    for timestamp, values in listed_timeline.items():
      index = int(self.quantile * 1.0 * (len(values) - 1) / 100.0)
      timeline[timestamp] = sorted(values)[index]
    result = Metrics(None, None, None, start, end, timeline)
    return [result]


class _SimpleArithmaticOperator(Operator):
  """
  This base class is for Subtract, Multiply, and Divide and gives the same conditions
  for each - to keep the API simple.

  Accepts two arguments, both can be univariate or multivariate.
  1. constant will be considered as a constant timeseries for all applicable timestamps
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.

  Three main cases are:
  1. When both operands are multivariate -
     a. Multiply operation will be done on matching data, that is, with same instance id.
     b. If the instances in both the operands do not match, error is thrown.
     c. Returns multivariate timeseries, each representing the result of multiplication
        on the two corresponding timeseries.
  2. When one operand is univariate, and other is multivariate -
     a. This includes multiplication by constants as well.
     b. The univariate operand will participate with all timeseries in multivariate.
     c. The instance information of the multivariate timeseries will be preserved in the result.
     d. Returns multivariate timeseries.
  3. When both operands are univariate.
     a. Instance information is ignored in this case
     b. Returns univariate timeseries which is the result of multiplication operation.

  """
  NAME = None

  # pylint: disable=super-init-not-called
  def __init__(self, children: list) -> None:
    if len(children) != 2:
      raise Exception(f"{self.NAME} requires exactly two arguments.")
    self.operand1: Union[float, Operator] = children[0]
    self.operand2: Union[float, Operator] = children[1]

  @classmethod
  async def _get_metrics(
      cls,
      operand: Union[float, Operator],
      tracker,
      tmanager: TManagerLocation,
      start: int,
      end: int,
  ) -> dict:
    result = {}
    if isinstance(operand, float):
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(operand, start, end)
      result[""] = met
    else:
      met = await operand.execute(tracker, tmanager, start, end)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        result[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception(f"{cls.NAME} with multivariate requires instance based timeseries")
          result[m.instance] = m
    return result

  @staticmethod
  def _is_multivariate(metrics: dict) -> bool:
    return len(metrics) > 1 or (len(metrics) == 1 and "" not in metrics)

  def _f(self, lhs: float, rhs: Optional[float]) -> None:
    """
    Return the result of this class' operation on two points.

    None is returned if the operands is unprocessable, such as if the rhs is None, or
    the rhs is 0 in Divide.

    """
    raise NotImplementedError()

  # pylint: disable=too-many-branches
  async def execute(self, tracker, tmanager: TManagerLocation, start: int, end: int) -> Any:
    """
    Return _f applied over all values in [start, end] of the two operands. Scalars
    are expanded a timeseries with all points equal to the scalar.

    """
    metrics, metrics2 = await asyncio.gather(
        self._get_metrics(self.operand1, tracker, tmanager, start, end),
        self._get_metrics(self.operand2, tracker, tmanager, start, end),
    )

    # In case both are multivariate, only equal instances will get operated
    if self._is_multivariate(metrics) and self._is_multivariate(metrics2):
      all_metrics = []
      for key in metrics:
        if key not in metrics2:
          continue
        met = Metrics(None, None, key, start, end, {})
        for timestamp in list(metrics[key].timeline.keys()):
          value = self._f(
              metrics[key].timeline[timestamp],
              metrics2[key].timeline.get(timestamp),
          )
          if value is None:
            metrics[key].timeline.pop(timestamp, None)
          else:
            met.timeline[timestamp] = value
        all_metrics.append(met)
      return all_metrics

    # If first is univariate
    if not self._is_multivariate(metrics):
      all_metrics = []
      for key, metric in metrics2.items():
        # Initialize with first metrics timeline, but second metric's instance
        # because that is multivariate
        if "" in metrics:
          met = Metrics(None, None, metric.instance, start, end, metrics[""].timeline.copy())
          for timestamp in list(met.timeline.keys()):
            v = self._f(met.timeline[timestamp], metric.timeline.get(timestamp))
            if v is None:
              met.timeline.pop(timestamp, None)
            else:
              met.timeline[timestamp] = v
          all_metrics.append(met)
      return all_metrics

    # If second is univariate
    all_metrics = []
    for key, metric in metrics.items():
      # Initialize with first metrics timeline and its instance
      met = Metrics(None, None, metric.instance, start, end, metric.timeline.copy())
      for timestamp in list(met.timeline.keys()):
        met2_value = None
        if "" in metrics2:
          met2_value = metrics2[""].timeline.get(timestamp)
        v = self._f(met.timeline[timestamp], met2_value)
        if v is None:
          met.timeline.pop(timestamp, None)
        else:
          met.timeline[timestamp] = v
      all_metrics.append(met)
    return all_metrics


class Multiply(_SimpleArithmaticOperator):
  NAME = "MULTIPLY"

  def _f(self, lhs: float, rhs: Optional[float]) -> None:
    if rhs is None:
      return None
    return lhs * rhs

class Subtract(_SimpleArithmaticOperator):
  NAME = "SUBTRACT"

  def _f(self, lhs: float, rhs: Optional[float]) -> None:
    if rhs is None:
      return None
    return lhs - rhs

class Divide(_SimpleArithmaticOperator):
  NAME = "DIVIDE"

  def _f(self, lhs: float, rhs: Optional[float]) -> None:
    if not rhs:
      return None
    return lhs / rhs


class Rate(Operator):
  """
  Rate Operator. This operator is used to find rate of change for all timeseries.
  Accepts a list of 1 element, which has to be a concrete subclass of Operators.
  Returns a list of Metrics object, representing rate of all timeseries

  """
  # pylint: disable=super-init-not-called
  def __init__(self, children) -> None:
    if len(children) != 1:
      raise Exception("RATE requires exactly one argument.")
    time_series, = children
    if not isinstance(time_series, Operator):
      raise Exception("RATE requires a timeseries, not constant.")
    self.time_series = time_series

  async def execute(self, tracker, tmanager: TManagerLocation, start: int, end: int) -> Any:
    # Get 1 previous data point to be able to apply rate on the first data
    metrics = await self.time_series.execute(tracker, tmanager, start-60, end)

    # Apply rate on all of them
    for metric in metrics:
      timeline: Dict[int, float] = {}
      all_timestamps = sorted(metric.timeline)
      for i, timestamp in enumerate(all_timestamps[1:], 1):
        prev = all_timestamps[i-1]
        if start <= timestamp <= end and timestamp - prev == 60:
          timeline[timestamp] = metric.timeline[timestamp] - metric.timeline[prev]
      metric.timeline = timeline
    return metrics
