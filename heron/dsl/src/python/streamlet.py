# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''streamlet.py: module for defining the basic concept of the heron python dsl'''
from collections import namedtuple
from heron.api.src.python import TopologyBuilder
from heron.api.src.python.component import GlobalStreamId
from heron.api.src.python.stream import Grouping

from .mapbolt import MapBolt
from .flatmapbolt import FlatMapBolt
from .filterbolt import FilterBolt
from .samplebolt import SampleBolt
from .joinbolt import JoinBolt
from .repartitionbolt import RepartitionBolt
from .reducebykeyandwindowbolt import ReduceByKeyAndWindowBolt

class OperationType(object):
  Input = 1
  Map = 2
  FlatMap = 3
  Filter = 4
  Sample = 5
  Join = 6
  Repartition = 7
  ReduceByKeyAndWindow = 8
  Output = 10

  AllOperations = [Input, Map, FlatMap, Filter, Sample, Join,
                   Repartition, ReduceByKeyAndWindow, Output]

  @staticmethod
  def valid(typ):
    return typ in OperationType.AllOperations

TimeWindow = namedtuple('TimeWindow', 'duration sliding_interval')

# pylint: disable=too-many-instance-attributes
class Streamlet(object):
  """A Streamlet is a (potentially unbounded) ordered collection of tuples
  """
  # pylint: disable=too-many-arguments
  # pylint: disable=too-many-function-args
  def __init__(self, parents=None, operation=None, stage_name=None,
               parallelism=None, inputs=None,
               map_function=None, flatmap_function=None,
               filter_function=None, time_window=None,
               sample_fraction=None, reduce_function=None):
    """
    """
    if operation is None:
      raise RuntimeError("Streamlet's operation cannot be None")
    if not OperationType.valid(operation):
      raise RuntimeError("Streamlet's operation must be of type OperationType")
    if operation == OperationType.Input:
      if parents is not None:
        raise RuntimeError("A input Streamlet's parents should be None")
    elif not isinstance(parents, list):
      raise RuntimeError("Streamlet's parents have to be a list")
    self._parents = parents
    self._operation = operation
    self._stage_name = stage_name
    self._parallelism = parallelism
    self._inputs = inputs
    self._output = 'output'
    self._map_function = map_function
    self._flatmap_function = flatmap_function
    self._filter_function = filter_function
    self._sample_fraction = sample_fraction
    self._time_window = time_window
    self._reduce_function = reduce_function

  def map(self, map_function, stage_name=None, parallelism=None):
    return Streamlet(parents=[self], operation=OperationType.Map,
                     stage_name=stage_name, parallelism=parallelism,
                     map_function=map_function)

  def flatMap(self, flatmap_function, stage_name=None, parallelism=None):
    return Streamlet(parents=[self], operation=OperationType.FlatMap,
                     stage_name=stage_name, parallelism=parallelism,
                     flatmap_function=flatmap_function)

  def filter(self, filter_function, stage_name=None, parallelism=None):
    return Streamlet(parents=[self], operation=OperationType.Filter,
                     stage_name=stage_name, parallelism=parallelism,
                     filter_function=filter_function)

  def sample(self, sample_fraction, stage_name=None, parallelism=None):
    return Streamlet(parents=[self], operation=OperationType.Sample,
                     stage_name=stage_name, parallelism=parallelism,
                     sample_fraction=sample_fraction)

  def repartition(self, parallelism, stage_name=None):
    return Streamlet(parents=[self], operation=OperationType.Repartition,
                     stage_name=stage_name, parallelism=parallelism)

  def join(self, join_streamlet, time_window, stage_name=None, parallelism=None):
    return Streamlet(parents=[self, join_streamlet], time_window=time_window,
                     operation=OperationType.Join,
                     stage_name=stage_name, parallelism=parallelism)

  def reduceByWindow(self, time_window, reduce_function, stage_name=None):
    return Streamlet(parents=[self], operation=OperationType.ReduceByKeyAndWindow,
                     stage_name=stage_name, parallelism=1,
                     time_window=time_window,
                     reduce_function=reduce_function)

  def reduceByKeyAndWindow(self, time_window, reduce_function, stage_name=None, parallelism=None):
    return Streamlet(parents=[self], operation=OperationType.ReduceByKeyAndWindow,
                     stage_name=stage_name, parallelism=parallelism,
                     time_window=time_window,
                     reduce_function=reduce_function)

  def run(self, name, config=None):
    if name is None or not isinstance(name, str):
      raise RuntimeError("Job Name has to be a string")
    bldr = TopologyBuilder(name=name)
    stage_names = {}
    bldr = self._build(bldr, stage_names)
    if config is not None:
      if not isinstance(config, dict):
        raise RuntimeError("config has to be a dict")
      bldr.set_config(config)
    bldr.build_and_submit()

  # pylint: disable=protected-access
  def _build(self, bldr, stage_names):
    for parent in self._parents:
      parent._build(bldr, stage_names)
    self._calculate_inputs()
    self._calculate_parallelism()
    return self._build_this(bldr, stage_names)

  # pylint: disable=too-many-branches
  def _build_this(self, builder, stage_names):
    if self._operation == OperationType.Input:
      raise RuntimeError("_build_this called from Input. Did the input implement it")
    elif self._operation == OperationType.Map:
      self.check_callable(self._map_function)
      self._generate_stage_name(stage_names, self._map_function, "map")
      builder.add_bolt(self._stage_name, MapBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={MapBolt.FUNCTION : self._map_function})
    elif self._operation == OperationType.FlatMap:
      self.check_callable(self._flatmap_function)
      self._generate_stage_name(stage_names, self._flatmap_function, "flatmap")
      builder.add_bolt(self._stage_name, FlatMapBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={FlatMapBolt.FUNCTION : self._flatmap_function})
    elif self._operation == OperationType.Filter:
      self.check_callable(self._filter_function)
      self._generate_stage_name(stage_names, self._filter_function, "filter")
      builder.add_bolt(self._stage_name, FilterBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={FilterBolt.FUNCTION : self._filter_function})
    elif self._operation == OperationType.Sample:
      if not isinstance(self._sample_fraction, float):
        raise RuntimeError("Sample Fraction has to be a float")
      if self._sample_fraction > 1.0:
        raise RuntimeError("Sample Fraction has to be <= 1.0")
      self._generate_sample_stage_name(stage_names)
      builder.add_bolt(self._stage_name, SampleBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={SampleBolt.FRACTION : self._sample_fraction})
    elif self._operation == OperationType.Join:
      self._generate_join_stage_name(stage_names)
      if not isinstance(self._time_window, TimeWindow):
        raise RuntimeError("Join's time_window should be TimeWindow")
      builder.add_bolt(self._stage_name, JoinBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={JoinBolt.WINDOWDURATION : self._time_window.duration,
                               JoinBolt.SLIDEINTERVAL : self._time_window.sliding_interval})
    elif self._operation == OperationType.Repartition:
      self._generate_repartition_stage_name(stage_names)
      builder.add_bolt(self._stage_name, RepartitionBolt, par=self._parallelism,
                       inputs=self._inputs)
    elif self._operation == OperationType.ReduceByKeyAndWindow:
      self.check_callable(self._reduce_function)
      self._generate_stage_name(stage_names, self._reduce_function, "reducebykeyandwindow")
      if not isinstance(self._time_window, TimeWindow):
        raise RuntimeError("ReduceByKeyAndWindow's time_window should be TimeWindow")
      builder.add_bolt(self._stage_name, ReduceByKeyAndWindowBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={ReduceByKeyAndWindowBolt.FUNCTION : self._reduce_function,
                               ReduceByKeyAndWindowBolt.WINDOWDURATION : self._time_window.duration,
                               ReduceByKeyAndWindowBolt.SLIDEINTERVAL :
                               self._time_window.sliding_interval})
    else:
      raise RuntimeError("Unknown type of operator", self._operation)

    return builder

  @staticmethod
  def check_callable(func):
    if not callable(func):
      raise RuntimeError("dsl functions must be callable")

  def _generate_stage_name(self, stage_names, func, functype):
    if self._stage_name is None:
      funcname = functype + "-" + func.__name__
      if funcname not in stage_names:
        self._stage_name = funcname
      else:
        index = 1
        while funcname in stage_names:
          index = index + 1
          funcname = functype + "-" + func.__name__ + str(index)
        self._stage_name = funcname
    elif self._stage_name in stage_names:
      raise RuntimeError("duplicated stage name %s" % self._stage_name)
    stage_names[self._stage_name] = 1

  def _generate_sample_stage_name(self, stage_names):
    if self._stage_name is None:
      self._stage_name = "sample"
      index = 1
      while self._stage_name in stage_names:
        index = index + 1
        self._stage_name = "sample" + str(index)
    elif self._stage_name in stage_names:
      raise RuntimeError("duplicated stage name %s" % self._stage_name)
    stage_names[self._stage_name] = 1


  # pylint: disable=protected-access
  def _generate_join_stage_name(self, stage_names):
    stage_name = self._parents[0]._stage_name
    for stage in self._parents[1:]:
      stage_name = stage_name + '.join.' + stage._stage_name
    if stage_name not in stage_names:
      self._stage_name = stage_name
    else:
      index = 1
      tmp_name = stage_name + str(index)
      while tmp_name in stage_names:
        index = index + 1
        tmp_name = stage_name + str(index)
      self._stage_name = tmp_name
    stage_names[self._stage_name] = 1

  def _generate_repartition_stage_name(self, stage_names):
    stage_name = "repartition"
    index = 1
    tmp_name = stage_name
    while tmp_name in stage_names:
      index = index + 1
      tmp_name = stage_name + str(index)
    self._stage_name = tmp_name
    stage_names[self._stage_name] = 1

  # pylint: disable=protected-access
  # pylint: disable=too-many-boolean-expressions
  def _calculate_parallelism(self):
    if self._parallelism is not None:
      return
    elif self._operation == OperationType.Map or \
         self._operation == OperationType.FlatMap or \
         self._operation == OperationType.Filter or \
         self._operation == OperationType.Sample or \
         self._operation == OperationType.Join or \
         self._operation == OperationType.ReduceByKeyAndWindow or \
         self._operation == OperationType.Output:
      parallelism = 1
      for parent in self._parents:
        if parent._parallelism > parallelism:
          parallelism = parent._parallelism
      self._parallelism = parallelism
    if self._parallelism is None:
      raise RuntimeError("Missed figuring out parallelism for", self._operation)

  # pylint: disable=protected-access
  # pylint: disable=fixme
  def _calculate_inputs(self):
    if self._operation == OperationType.Input:
      self._inputs = None
    elif self._operation == OperationType.Map or \
         self._operation == OperationType.FlatMap or \
         self._operation == OperationType.Filter or \
         self._operation == OperationType.Sample or \
         self._operation == OperationType.Repartition:
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
                      Grouping.SHUFFLE}
    elif self._operation == OperationType.Join:
      self._inputs = {}
      for parent in self._parents:
        self._inputs[GlobalStreamId(parent._stage_name, parent._output)] = \
                     Grouping.custom("heron.dsl.src.python.joinbolt.JoinGrouping")
    elif self._operation == OperationType.ReduceByKeyAndWindow:
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
                      Grouping.custom(\
                      "heron.dsl.src.python.reducebykeyandwindowbolt.ReduceGrouping")}
    elif self._operation == OperationType.Output:
      # FIXME:- is this correct
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._output) :
                      Grouping.SHUFFLE}
