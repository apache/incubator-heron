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

"""metrics.py: common heron metric"""
from abc import abstractmethod

# pylint: disable=attribute-defined-outside-init

class IMetric(object):
  """Interface for Heron Metric"""
  @abstractmethod
  def get_value_and_reset(self):
    """Returns the current value and reset"""
    pass

class CountMetric(IMetric):
  """Counter for a single value"""
  def __init__(self):
    """Initializes CountMetric"""
    self.value = 0

  def incr(self, to_add=1):
    """Increments the value by ``to_add``"""
    self.value += to_add

  def get_value_and_reset(self):
    ret = self.value
    self.value = 0
    return ret

class MultiCountMetric(IMetric):
  """Counter for a multiple value"""
  def __init__(self):
    """Initializes MultiCountMetric"""
    self.value = {}

  def add_key(self, key):
    """Registers a new key"""
    self.incr(key, to_add=0)

  def incr(self, key, to_add=1):
    """Increments the value of a given key by ``to_add``"""
    if key not in self.value:
      self.value[key] = CountMetric()
    self.value[key].incr(to_add)

  def get_value_and_reset(self):
    ret = {}
    for key, value in list(self.value.items()):
      ret[key] = value.get_value_and_reset()
    return ret

# Reducer metric
class IReducer(object):
  """Interface for Reducer"""
  @abstractmethod
  def init(self):
    """Called when this reducer is initialized/reinitialized"""
    pass

  @abstractmethod
  def reduce(self, value):
    """Called to reduce the value"""
    pass

  @abstractmethod
  def extract(self):
    """Called to extract the current value"""
    pass

class MeanReducer(IReducer):
  """Mean Reducer"""
  def init(self):
    self.sum = 0
    self.count = 0

  def reduce(self, value):
    self.sum += float(value)
    self.count += 1

  def extract(self):
    if self.count > 0:
      return float(self.sum)/self.count
    else:
      return None

class ReducedMetric(IMetric):
  """Reduced Metric"""
  def __init__(self, reducer_cls):
    """Initializes ReducedMetric object

    :param reducer_cls: IReducer class to use
    """
    self.reducer = reducer_cls()
    self.reducer.init()

  def update(self, value):
    """Updates a value and apply reduction"""
    self.reducer.reduce(value)

  def get_value_and_reset(self):
    value = self.reducer.extract()
    self.reducer.init()
    return value

class MultiReducedMetric(IMetric):
  """MultiReducedMetric"""
  def __init__(self, reducer):
    """Initializes MultiReducedMetric object

    :param reducer: IReducer class to use
    """
    self.value = {}
    self.reducer = reducer

  def update(self, key, value):
    """Updates a value of a given key and apply reduction"""
    if key not in self.value:
      self.value[key] = ReducedMetric(self.reducer)

    self.value[key].update(value)

  def add_key(self, key):
    """Adds a new key to this metric"""
    if key not in self.value:
      self.value[key] = ReducedMetric(self.reducer)

  def get_value_and_reset(self):
    ret = {}
    for key, value in list(self.value.items()):
      ret[key] = value.get_value_and_reset()
      self.value[key] = value
    return ret

class AssignableMetrics(IMetric):
  """AssignableMetrics"""
  def __init__(self, init_val):
    self.value = init_val

  def update(self, value):
    self.value = value

  def get_value_and_reset(self):
    return self.value

class MultiAssignableMetrics(IMetric):
  """MultiAssignableMetrics"""
  def __init__(self):
    self.map = {}

  def update(self, key, value):
    if key not in self.map:
      self.map[key] = AssignableMetrics(value)
    else:
      self.map[key].update(value)

  def get_value_and_reset(self):
    ret = {}
    for k in self.map:
      ret[k] = self.map[k].get_value_and_reset()
    return ret

MeanReducedMetric = lambda: ReducedMetric(MeanReducer)
MultiMeanReducedMetric = lambda: MultiReducedMetric(MeanReducer)
