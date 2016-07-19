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
from abc import abstractmethod


class IMetric(object):
  @abstractmethod
  def get_value_and_reset(self):
    pass

class CountMetric(IMetric):
  def __init__(self):
    self.value = 0

  def incr(self, to_add=1):
    self.value += to_add

  def get_value_and_reset(self):
    ret = self.value
    self.value = 0
    return ret

class MultiCountMetric(IMetric):
  def __init__(self):
    self.value = {}

  def add_key(self, key):
    self.incr(key, to_add=0)

  def incr(self, key, to_add=1):
    if key not in self.value:
      self.value[key] = CountMetric()
    self.value[key].incr(to_add)

  def get_value_and_reset(self):
    ret = {}
    for key, value in self.value.iteritems():
      ret[key] = value.get_value_and_reset()
    return ret


# Reducer metric

class IReducer(object):
  @abstractmethod
  def init(self):
    pass

  @abstractmethod
  def reduce(self, value):
    pass

  @abstractmethod
  def extract(self):
    pass

class MeanReducer(IReducer):
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
  def __init__(self, reducer_cls):
    self.reducer = reducer_cls()
    self.reducer.init()

  def update(self, value):
    self.reducer.reduce(value)

  def get_value_and_reset(self):
    value = self.reducer.extract()
    self.reducer.init()
    return value

class MultiReducedMetric(IMetric):
  def __init__(self, reducer):
    self.value = {}
    self.reducer = reducer

  def update(self, key, value):
    if key not in self.value:
      self.value[key] = ReducedMetric(self.reducer)

    self.value[key].update(value)

  def add_key(self, key):
    if key not in self.value:
      self.value[key] = ReducedMetric(self.reducer)

  def get_value_and_reset(self):
    ret = {}
    for key, value in self.value.iteritems():
      ret[key] = value.get_value_and_reset()
    return ret


MeanReducedMetric = lambda : ReducedMetric(MeanReducer)
MultiMeanReducedMetric = lambda : MultiReducedMetric(MeanReducer)
