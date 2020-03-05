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

''' query_operators.py '''
import sys
import math
import tornado.httpclient
import tornado.gen

from heron.tools.tracker.src.python.metricstimeline import getMetricsTimeline

isPY3 = sys.version_info >= (3, 0, 0)

# helper method to support python 2 and 3
def is_str_instance(obj):
  if isPY3:
    return isinstance(obj, str)
  else:
    return str(type(obj)) == "<type 'unicode'>" or str(type(obj)) == "<type 'str'>"

#####################################################################
# Data Structure for fetched Metrics
#####################################################################
class Metrics(object):
  """Represents a univariate timeseries.
  Multivariate timeseries is simply a list of this."""
  def __init__(self, componentName, metricName, instance, start, end, timeline):
    """Takes (componentName, metricname, instance, timeline)"""
    self.componentName = componentName
    self.metricName = metricName
    self.instance = instance
    self.start = start
    self.end = end
    self.timeline = self.floorTimestamps(start, end, timeline)

  # pylint: disable=no-self-use
  def floorTimestamps(self, start, end, timeline):
    """ floor timestamp """
    ret = {}
    for timestamp, value in list(timeline.items()):
      ts = timestamp / 60 * 60
      if start <= ts <= end:
        ret[ts] = value
    return ret

  def setDefault(self, constant, start, end):
    """ set default time """
    starttime = start / 60 * 60
    if starttime < start:
      starttime += 60
    endtime = end / 60 * 60
    while starttime <= endtime:
      # STREAMCOMP-1559
      # Second check is a work around, because the response from tmaster
      # contains value 0, if it is queries for the current timestamp,
      # since the bucket is created in the tmaster, but is not filled
      # by the metrics.
      if starttime not in self.timeline or self.timeline[starttime] == 0:
        self.timeline[starttime] = constant
      starttime += 60

################################################################
# All the Operators supported by query system.
################################################################

# pylint: disable=no-self-use
class Operator(object):
  """Base class for all operators"""
  def __init__(self, _):
    raise Exception("Not implemented exception")

  # pylint: disable=unused-argument
  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    """ execute """
    raise Exception("Not implemented exception")

  def isOperator(self):
    """Returns True. This is just usefule for checking that an object is an operator or not."""
    return True


class TS(Operator):
  """Time Series Operator. This is the basic operator that is
  responsible for getting metrics from tmaster.
  Accepts a list of 3 elements:
  1. componentName
  2. instance - can be "*" for all instances, or a single instance ID
  3. metricName - Full metric name with stream id if applicable
  Returns a list of Metrics objects, each representing single timeseries"""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 3:
      raise Exception("TS format error, expects 3 arguments")
    self.component = children[0]
    if not is_str_instance(self.component):
      raise Exception("TS expects component name as first argument")
    # A '*' represents all instances, which is represented by empty array.
    # Otherwise, it represents a single instance
    self.instances = []
    if children[1] != "*":
      if not is_str_instance(children[1]):
        raise Exception("Second argument of TS must be * or instance name")
      self.instances.append(children[1])
    self.metricName = children[2]
    if not is_str_instance(self.metricName):
      raise Exception("TS expects metric name as third argument")

  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Fetch metrics for start-60 to end+60 because the minute mark
    # may be a little skewed. By getting a couple more values,
    # we can then truncate based on the interval needed.
    metrics = yield getMetricsTimeline(
        tmaster, self.component, [self.metricName], self.instances,
        start - 60, end + 60)
    if not metrics:
      return
    if "message" in metrics:
      raise Exception(metrics["message"])

    # Put a blank timeline.
    if "timeline" not in metrics or not metrics["timeline"]:
      metrics["timeline"] = {
          self.metricName: {}
      }
    timelines = metrics["timeline"][self.metricName]
    allMetrics = []
    for instance, timeline in list(timelines.items()):
      toBeDeletedKeys = []
      for key, value in list(timeline.items()):
        floatValue = float(value)
        # Check if the value is really float or not.
        # In python, float("nan") returns "nan" which is actually a float value,
        # but it is not what we required.
        if math.isnan(floatValue):
          toBeDeletedKeys.append(key)
          continue
        timeline[key] = floatValue

      # Remove all keys for which the value was not float
      for key in toBeDeletedKeys:
        timeline.pop(key)

      allMetrics.append(Metrics(self.component, self.metricName, instance, start, end, timeline))
    raise tornado.gen.Return(allMetrics)

class Default(Operator):
  """Default Operator. This operator is responsible for filling
  holes in the metrics timeline of its children.
  Accepts a list of 2 elements:
  1. constant to fill the holes with
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of Metrics objects, each representing single timeseries"""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 2:
      raise Exception("DEFAULT format error, expects 2 arguments")
    if not isinstance(children[0], float):
      raise Exception("First argument to DEFAULT must be a constant")
    self.constant = children[0]
    self.timeseries = children[1]
    if not self.timeseries.isOperator():
      raise Exception(
          "Second argument to DEFAULT must be an operator, but is " + str(type(self.timeseries)))

  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    allMetrics = yield self.timeseries.execute(tracker, tmaster, start, end)
    if is_str_instance(allMetrics):
      raise Exception(allMetrics)
    for metric in allMetrics:
      metric.setDefault(self.constant, start, end)
    raise tornado.gen.Return(allMetrics)

class Sum(Operator):
  """Sum Operator. This operator is used to take sum of all children timeseries.
  Accepts a list of elements, all of which have to be either constant or Operators.
  Note that the length of the children is unbounded.
  1. constants will fill in the holes as well, if present in other timeseries
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of only one Metrics object, representing sum of all timeseries"""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    self.timeSeriesList = children

  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Initialize the metric to be returned with sum of all the constants.
    retMetrics = Metrics(None, None, None, start, end, {})
    constants = [ts for ts in self.timeSeriesList if isinstance(ts, float)]
    retMetrics.setDefault(sum(constants), start, end)
    leftOverTimeSeries = [ts for ts in self.timeSeriesList if not isinstance(ts, float)]

    futureMetrics = []
    for timeseries in leftOverTimeSeries:
      futureMetrics.append(timeseries.execute(tracker, tmaster, start, end))

    metrics = yield futureMetrics
    # Get all the timeseries metrics
    allMetrics = []
    for met in metrics:
      if is_str_instance(met):
        raise Exception(met)
      allMetrics.extend(met)

    # Aggregate all of the them
    for metric in allMetrics:
      for timestamp, value in list(metric.timeline.items()):
        if timestamp in retMetrics.timeline:
          retMetrics.timeline[timestamp] += value
    raise tornado.gen.Return([retMetrics])

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
    self.timeSeriesList = children

  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Initialize the metric to be returned with max of all the constants.
    retMetrics = Metrics(None, None, None, start, end, {})
    constants = [ts for ts in self.timeSeriesList if isinstance(ts, float)]
    if constants:
      retMetrics.setDefault(max(constants), start, end)
    leftOverTimeSeries = [ts for ts in self.timeSeriesList if not isinstance(ts, float)]

    futureMetrics = []
    for timeseries in leftOverTimeSeries:
      futureMetrics.append(timeseries.execute(tracker, tmaster, start, end))

    metrics = yield futureMetrics

    # Get all the timeseries metrics
    allMetrics = []
    for met in metrics:
      if is_str_instance(met):
        raise Exception(met)
      allMetrics.extend(met)

    # Aggregate all of the them
    for metric in allMetrics:
      for timestamp, value in list(metric.timeline.items()):
        if start <= timestamp <= end:
          if timestamp not in retMetrics.timeline:
            retMetrics.timeline[timestamp] = value
          retMetrics.timeline[timestamp] = max(value, retMetrics.timeline[timestamp])
    raise tornado.gen.Return([retMetrics])

class Percentile(Operator):
  """Percentile Operator. This operator is used to find a quantile of all children
  timeseries for each individual timestamp. This is a more general type of query
  than max. Percentile(100, TS...) is equivalent to Max(TS...).
  Accepts a list of elements, all of which have to be either constant or Operators.
  Note that the length of the children is unbounded.
  First argument must always be the required Quantile.
  1. Quantile - Required quantile. 100 percentile = max, 0 percentile = min.
  2. constants will fill in the holes as well, if present in other timeseries
  3. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.
  Returns a list of only one Metrics object, representing quantile of all timeseries"""

  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) < 1:
      raise Exception("PERCENTILE expects at least two operands.")
    if not isinstance(children[0], float):
      raise Exception("First argument to PERCENTILE must be a constant")
    if not 0 <= children[0] <= 100:
      raise Exception("Quantile must be between 0 and 100 inclusive.")
    self.quantile = children[0]
    self.timeSeriesList = children[1:]

  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    leftOverTimeSeries = [ts for ts in self.timeSeriesList if not isinstance(ts, float)]

    futureMetrics = []
    for timeseries in leftOverTimeSeries:
      futureMetrics.append(timeseries.execute(tracker, tmaster, start, end))

    metrics = yield futureMetrics

    # Get all the timeseries metrics
    allMetrics = []
    for met in metrics:
      if is_str_instance(met):
        raise Exception(met)
      allMetrics.extend(met)

    # Keep all the values for a timestamp and we will later do
    # a percentile on it
    timeline = {}

    # Aggregate all of the them
    for metric in allMetrics:
      for timestamp, value in list(metric.timeline.items()):
        if start <= timestamp <= end:
          if timestamp not in timeline:
            timeline[timestamp] = []
          timeline[timestamp].append(value)

    retTimeline = {}
    for timestamp, values in list(timeline.items()):
      if not values:
        continue
      index = int(self.quantile * 1.0 * (len(values) - 1) / 100.0)
      retTimeline[timestamp] = sorted(values)[index]
    retMetrics = Metrics(None, None, None, start, end, retTimeline)
    raise tornado.gen.Return([retMetrics])


class Divide(Operator):
  """Divide Operator.
  Accepts two arguments, both can be univariate or multivariate.
  1. constant will be considered as a constant timeseries for all applicable timestamps
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.

  Three main cases are:
  1. When both operands are multivariate -
     a. Divide operation will be done on matching data, that is, with same instance id.
     b. If the instances in both the operands do not match, error is thrown.
     c. Returns multivariate timeseries, each representing the result of division
        on the two corresponding timeseries.
  2. When one operand is univariate, and other is multivariate -
     a. This includes division by constants as well.
     b. The univariate operand will participate with all timeseries in multivariate.
     c. The instance information of the multivariate timeseries will be preserved in the result.
     d. Returns multivariate timeseries.
  3. When both operands are univariate.
     a. Instance information is ignored in this case
     b. Returns univariate timeseries which is the result of division operation."""

  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 2:
      raise Exception("DIVIDE expects exactly two arguments.")
    self.timeSeries1 = children[0]
    self.timeSeries2 = children[1]

  # pylint: disable=too-many-branches, too-many-statements
  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Future metrics so as to execute them in parallel
    futureMetrics = []
    if not isinstance(self.timeSeries1, float):
      futureMetrics.append(self.timeSeries1.execute(tracker, tmaster, start, end))
    if not isinstance(self.timeSeries2, float):
      futureMetrics.append(self.timeSeries2.execute(tracker, tmaster, start, end))

    futureResolvedMetrics = yield futureMetrics

    # Get first set of metrics
    metrics = {}
    if isinstance(self.timeSeries1, float):
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(self.timeSeries1, start, end)
      metrics[""] = met
    else:
      met = futureResolvedMetrics.pop(0)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        metrics[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception("DIVIDE with multivariate requires instance based timeseries")
          metrics[m.instance] = m

    # Get second set of metrics
    metrics2 = {}
    if isinstance(self.timeSeries2, float):
      if self.timeSeries2 == 0:
        raise Exception("Divide by zero not allowed")
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(self.timeSeries2, start, end)
      metrics2[""] = met
    else:
      met = futureResolvedMetrics.pop(0)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        metrics2[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception("DIVIDE with multivariate requires instance based timeseries")
          metrics2[m.instance] = m

    # In case both are multivariate, only equal instances will get operated on.
    # pylint: disable=too-many-boolean-expressions
    if ((len(metrics) > 1 or (len(metrics) == 1 and "" not in metrics))
        and (len(metrics2) > 1 or (len(metrics2) == 1 and "" not in metrics2))):
      allMetrics = []
      for key in metrics:
        if key not in metrics2:
          continue
        met = Metrics(None, None, key, start, end, {})
        for timestamp in list(metrics[key].timeline.keys()):
          if timestamp not in metrics2[key].timeline or metrics2[key].timeline[timestamp] == 0:
            metrics[key].timeline.pop(timestamp)
          else:
            met.timeline[timestamp] = metrics[key].timeline[timestamp] / \
              metrics2[key].timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    # If first is univariate
    elif len(metrics) == 1 and "" in metrics:
      allMetrics = []
      for key, metric in list(metrics2.items()):
        # Initialize with first metrics timeline, but second metric's instance
        # because that is multivariate
        met = Metrics(None, None, metric.instance, start, end, dict(metrics[""].timeline))
        for timestamp in list(met.timeline.keys()):
          if timestamp not in metric.timeline or metric.timeline[timestamp] == 0:
            met.timeline.pop(timestamp)
          else:
            met.timeline[timestamp] /= metric.timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    # If second is univariate
    else:
      allMetrics = []
      for key, metric in list(metrics.items()):
        # Initialize with first metrics timeline and its instance
        met = Metrics(None, None, metric.instance, start, end, dict(metric.timeline))
        for timestamp in list(met.timeline.keys()):
          if timestamp not in metrics2[""].timeline or metrics2[""].timeline[timestamp] == 0:
            met.timeline.pop(timestamp)
          else:
            met.timeline[timestamp] /= metrics2[""].timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    raise Exception("This should not be generated.")

class Multiply(Operator):
  """Multiply Operator. Has same conditions as division operator.
  This is to keep the API simple.
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
     b. Returns univariate timeseries which is the result of multiplication operation."""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 2:
      raise Exception("MULTIPLY expects exactly two arguments.")
    self.timeSeries1 = children[0]
    self.timeSeries2 = children[1]

  # pylint: disable=too-many-branches, too-many-statements
  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Future metrics so as to execute them in parallel
    futureMetrics = []
    if not isinstance(self.timeSeries1, float):
      futureMetrics.append(self.timeSeries1.execute(tracker, tmaster, start, end))
    if not isinstance(self.timeSeries2, float):
      futureMetrics.append(self.timeSeries2.execute(tracker, tmaster, start, end))

    futureResolvedMetrics = yield futureMetrics

    # Get first set of metrics
    metrics = {}
    if isinstance(self.timeSeries1, float):
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(self.timeSeries1, start, end)
      metrics[""] = met
    else:
      met = futureResolvedMetrics.pop(0)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        metrics[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception("MULTIPLY with multivariate requires instance based timeseries")
          metrics[m.instance] = m

    # Get second set of metrics
    metrics2 = {}
    if isinstance(self.timeSeries2, float):
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(self.timeSeries2, start, end)
      metrics2[""] = met
    else:
      met = futureResolvedMetrics.pop(0)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        metrics2[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception("MULTIPLY with multivariate requires instance based timeseries")
          metrics2[m.instance] = m

    # In case both are multivariate, only equal instances will get operated
    # pylint: disable=too-many-boolean-expressions
    if ((len(metrics) > 1 or (len(metrics) == 1 and "" not in metrics))
        and (len(metrics2) > 1 or (len(metrics2) == 1 and "" not in metrics2))):
      allMetrics = []
      for key in metrics:
        if key not in metrics2:
          continue
        met = Metrics(None, None, key, start, end, {})
        for timestamp in list(metrics[key].timeline.keys()):
          if timestamp not in metrics2[key].timeline:
            metrics[key].timeline.pop(timestamp)
          else:
            met.timeline[timestamp] = metrics[key].timeline[timestamp] * \
              metrics2[key].timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    # If first is univariate
    elif len(metrics) == 1 and "" in metrics:
      allMetrics = []
      for key, metric in list(metrics2.items()):
        # Initialize with first metrics timeline, but second metric's instance
        # because that is multivariate
        met = Metrics(None, None, metric.instance, start, end, dict(metrics[""].timeline))
        for timestamp in list(met.timeline.keys()):
          if timestamp not in metric.timeline:
            met.timeline.pop(timestamp)
          else:
            met.timeline[timestamp] *= metric.timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    # If second is univariate
    else:
      allMetrics = []
      for key, metric in list(metrics.items()):
        # Initialize with first metrics timeline and its instance
        met = Metrics(None, None, metric.instance, start, end, dict(metric.timeline))
        for timestamp in list(met.timeline.keys()):
          if timestamp not in metrics2[""].timeline:
            met.timeline.pop(timestamp)
          else:
            met.timeline[timestamp] *= metrics2[""].timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    raise Exception("This should not be generated.")

class Subtract(Operator):
  """Subtract Operator. Has same conditions as division operator.
  This is to keep the API simple.
  Accepts two arguments, both can be univariate or multivariate.
  1. constant will be considered as a constant timeseries for all applicable timestamps
  2. Operator - can be any concrete subclass of Operator on which "execute" can
     be called which returns a list of Metrics.

  Three main cases are:
  1. When both operands are multivariate -
     a. Subtract operation will be done on matching data, that is, with same instance id.
     b. If the instances in both the operands do not match, error is thrown.
     c. Returns multivariate timeseries, each representing the result of subtraction
        on the two corresponding timeseries.
  2. When one operand is univariate, and other is multivariate -
     a. This includes subtraction by constants as well.
     b. The univariate operand will participate with all timeseries in multivariate.
     c. The instance information of the multivariate timeseries will be preserved in the result.
     d. Returns multivariate timeseries.
  3. When both operands are univariate.
     a. Instance information is ignored in this case
     b. Returns univariate timeseries which is the result of subtraction operation."""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 2:
      raise Exception("SUBTRACT expects exactly two arguments.")
    self.timeSeries1 = children[0]
    self.timeSeries2 = children[1]

  # pylint: disable=too-many-branches, too-many-statements
  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Future metrics so as to execute them in parallel
    futureMetrics = []
    if not isinstance(self.timeSeries1, float):
      futureMetrics.append(self.timeSeries1.execute(tracker, tmaster, start, end))
    if not isinstance(self.timeSeries2, float):
      futureMetrics.append(self.timeSeries2.execute(tracker, tmaster, start, end))

    futureResolvedMetrics = yield futureMetrics

    # Get first set of metrics
    metrics = {}
    if isinstance(self.timeSeries1, float):
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(self.timeSeries1, start, end)
      metrics[""] = met
    else:
      met = futureResolvedMetrics.pop(0)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        metrics[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception("SUBTRACT with multivariate requires instance based timeseries")
          metrics[m.instance] = m

    # Get second set of metrics
    metrics2 = {}
    if isinstance(self.timeSeries2, float):
      met = Metrics(None, None, None, start, end, {})
      met.setDefault(self.timeSeries2, start, end)
      metrics2[""] = met
    else:
      met = futureResolvedMetrics.pop(0)
      if not met:
        pass
      elif len(met) == 1 and not met[0].instance:
        # Only one but since it has instance, it is considered multivariate
        metrics2[""] = met[0]
      else:
        for m in met:
          if not m.instance:
            raise Exception("SUBTRACT with multivariate requires instance based timeseries")
          metrics2[m.instance] = m

    # In case both are multivariate, only equal instances will get operated on.
    if len(metrics) > 1 and len(metrics2) > 1:
      allMetrics = []
      for key in metrics:
        if key not in metrics2:
          continue
        met = Metrics(None, None, key, start, end, {})
        for timestamp in list(metrics[key].timeline.keys()):
          if timestamp not in metrics2[key].timeline:
            metrics[key].timeline.pop(timestamp)
          else:
            met.timeline[timestamp] = metrics[key].timeline[timestamp] - \
              metrics2[key].timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    # If first is univariate
    elif len(metrics) == 1 and "" in metrics:
      allMetrics = []
      for key, metric in list(metrics2.items()):
        # Initialize with first metrics timeline, but second metric's instance
        # because that is multivariate
        met = Metrics(None, None, metric.instance, start, end, dict(metrics[""].timeline))
        for timestamp in list(met.timeline.keys()):
          if timestamp not in metric.timeline:
            met.timeline.pop(timestamp)
          else:
            met.timeline[timestamp] -= metric.timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    # If second is univariate
    else:
      allMetrics = []
      for key, metric in list(metrics.items()):
        # Initialize with first metrics timeline and its instance
        met = Metrics(None, None, metric.instance, start, end, dict(metric.timeline))
        for timestamp in list(met.timeline.keys()):
          if timestamp not in metrics2[""].timeline:
            met.timeline.pop(timestamp)
          else:
            met.timeline[timestamp] -= metrics2[""].timeline[timestamp]
        allMetrics.append(met)
      raise tornado.gen.Return(allMetrics)
    raise Exception("This should not be generated.")

class Rate(Operator):
  """Rate Operator. This operator is used to find rate of change for all timeseries.
  Accepts a list of 1 element, which has to be a concrete subclass of Operators.
  Returns a list of Metrics object, representing rate of all timeseries"""
  # pylint: disable=super-init-not-called
  def __init__(self, children):
    if len(children) != 1:
      raise Exception("RATE expects exactly one argument.")
    if isinstance(children[0], float):
      raise Exception("RATE requires a timeseries, not constant.")
    self.timeSeries = children[0]

  @tornado.gen.coroutine
  def execute(self, tracker, tmaster, start, end):
    # Get 1 previous data point to be able to apply rate on the first data
    metrics = yield self.timeSeries.execute(tracker, tmaster, start-60, end)

    # Apply rate on all of them
    for metric in metrics:
      timeline = {}
      allTimestamps = sorted(metric.timeline.keys())
      for i in range(1, len(allTimestamps)):
        timestamp = allTimestamps[i]
        prev = allTimestamps[i-1]
        if start <= timestamp <= end and timestamp - prev == 60:
          timeline[timestamp] = metric.timeline[timestamp] - metric.timeline[prev]
      metric.timeline = timeline
    raise tornado.gen.Return(metrics)
