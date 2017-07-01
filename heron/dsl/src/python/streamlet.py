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

class StreamletBuilder(object):
  """A Streamlet is a (potentially unbounded) ordered collection of tuples
  """
  class OperationType:
    Input, Map, FlatMap, Filter, Sample,
    Join, Repartition, ReduceByWindow, ReduceByKeyAndWindow, Output = range(10)

  def __init__(self, parents=[], operation=None, stage_name=None,
               parallelism=None, inputs=None, oututs=None,
               map_function=None, flatmap_function=None,
               filter_function=None, window_config=None,
               sample_function=None, join_streamlet=None,
               reduce_function=None):
    """
    """
    if operation is None:
      raise RuntimeError("Streamlet's operation cannot be None")
    self._parents = parents
    self._operation = operation
    self._stage_name = stage_name
    self._parallelism = parallelism
    self._inputs = inputs
    self._outputs = outputs
    self._map_function = map_function
    self._flatmap_function = flatmap_function
    self._filter_function = filter_function
    self._sample_function = sample_function
    self._join_streamlet = join_streamlet
    self._window_config = window_config
    self._reduce_function = reduce_function

  def map(self, map_function, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self], operation=OperationType.Map,
                            stage_name=stage_name, parallelism=parallelism,
                            map_function=map_function)

  def flatMap(self, flatmap_function, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self], operation=OperationType.FlatMap,
                            stage_name=stage_name, parallelism=parallelism,
                            flatmap_function=flatmap_function)

  def filter(self, filter_function, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self], operation=OperationType.Filter,
                            stage_name=stage_name, parallelism=parallelism,
                            filter_function=filter_function)

  def sample(self, sample_function, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self], operation=OperationType.Sample,
                            stage_name=stage_name, parallelism=parallelism,
                            sample_function=sample_function)

  def repartition(self, stage_name=None, parallelism):
    return StreamletBuilder(parents=[self], operation=OperationType.Repartition,
                            stage_name=stage_name, parallelism=parallelism)

  def join(self, join_streamlet, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self, join_streamlet], operation=OperationType.Join,
                            stage_name=stage_name, parallelism=parallelism,
                            join_streamlet=join_streamlet)

  def reduceByWindow(self, window_config, reduce_function, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self], operation=OperationType.ReduceByWindow,
                            stage_name=stage_name, parallelism=parallelism,
                            window_config=window_config,
                            reduce_function=reduce_function)

  def reduceByKeyAndWindow(self, window_config, reduce_function, stage_name=None, parallelism=None):
    return StreamletBuilder(parents=[self], operation=OperationType.ReduceByKeyAndWindow,
                            stage_name=stage_name, parallelism=parallelism,
                            window_config=window_config,
                            reduce_function=reduce_function)

  def build(self, name):
    bldr = TopologyBuilder(name=name)
    stage_names = {}
    return self._build(bldr, state_names)

  def _build(bldr, stage_names):
    for parent in self._parents:
      parent._build(bldr, stage_names)
    self._calculate_inputs()
    self._calculate_parallelism()
    return _build_this(bldr, stage_names)

  def _build_this(self, bldr, stage_names):
    if self._operation == OperationType.Input:
      raise RuntimeError("_build_this called from Input. Did the input implement it")
    elif self._operation == OperationType.Map:
      check_callable(self._map_function)
      self._generate_stage_name(stage_names, self._map_function)
      builder.add_bolt(self._stage_name, MapBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={MapBolt.FUNCTION : self._map_function})
    elif self._operation == OperationType.FlatMap:
      check_callable(self._flatmap_function)
      self._generate_stage_name(stage_names, self._flatmap_function)
      builder.add_bolt(self._stage_name, FlatMapBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={FlatMapBolt.FUNCTION : self._flatmap_function})
    elif self._operation == OperationType.Filter:
      check_callable(self._filter_function)
      self._generate_stage_name(stage_names, self._filter_function)
      builder.add_bolt(self._stage_name, FilterBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={FilterBolt.FUNCTION : self._filter_function})
    elif self._operation == OperationType.Sample:
      check_callable(self._sample_function)
      self._generate_stage_name(stage_names, self._sample_function)
      builder.add_bolt(self._stage_name, SampleBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={SampleBolt.FUNCTION : self._sample_function})
    elif self._operation == OperationType.Join:
      self._generate_join_stage_name(stage_names)
      builder.add_bolt(self._stage_name, JoinBolt, par=self._parallelism,
                       inputs=self._inputs)
    elif self._operation == OperationType.Repartition:
      self._generate_repartition_stage_name(stage_names)
      builder.add_bolt(self._stage_name, RepartitionBolt, par=self._parallelism,
                       inputs=self._inputs)
    elif self._operation == OperationType.ReduceByWindow:
      check_callable(self._reduce_function)
      self._generate_stage_name(stage_names, self._reduce_function)
      builder.add_bolt(self._stage_name, ReduceByWindowBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={ReduceByWindowBolt.FUNCTION : self._reduce_function,
                               ReduceByWindowBolt.WINDOW_CONFIG : self._window_config})
    elif self._operation == OperationType.ReduceByKeyAndWindow:
      check_callable(self._reduce_function)
      self._generate_stage_name(stage_names, self._reduce_function)
      builder.add_bolt(self._stage_name, ReduceByWindowBolt, par=self._parallelism,
                       inputs=self._inputs,
                       config={ReduceByKeyAndWindowBolt.FUNCTION : self._reduce_function,
                               ReduceByKeyAndWindowBolt.WINDOW_CONFIG : self._window_config})
    else:
      raise RuntimeError("Unknown type of operator", self._operation)

  @staticmethod
  def check_callable(func):
    if not callable(func):
      raise RuntimeError("dsl functions must be callable")

  def _generate_stage_name(self, stage_names, func):
    if self._stage_name is None:
      funcname = func.__name__
      if funcname not in stage_names:
        self._stage_name = funcname
      else:
        index = 1
        while funcname in stage_names:
          index = index + 1
          funcname = func.__name__ + str(index)
       self._stage_name = funcname
    elif self._stage_name in stage_names:
      raise RuntimeError("duplicated stage name %s" % self._stage_name)
    stage_names[self._stage_name] = 1

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
      tmp_name = stage_name + str(imndex)
    self._stage_name = tmp_name
    stage_names[self._stage_name] = 1

  def _calculate_parallelism(self):
    if self._parallelism is not None:
      return
    if self._operation == OperationType.Map ||
       self._operation == OperationType.FlatMap ||
       self._operation == OperationType.Filter ||
       self._operation == OperationType.Sample ||
       self._operation == OperationType.Join ||
       self._operation == OperationType.ReduceByKeyAndWindow ||
       self._operation == OperationType.Output:
      parallelism = 1
      for parent in self.parents:
        if parent._parallelism > parallelism:
          parallelism = parent._parallelism
      self._parallelism = parallelism
    elif self._operation == OperationType.ReduceByWindow:
      self._parallelism = 1
    if self._parallelism is None:
      raise RuntimeError("Missed figuring out parallelism for", self._operation)

  def _calculate_inputs(self):
    if self._operation == OperationType.Input:
      self._inputs = None
    elif self._operation == OperationType.Map ||
         self._operation == OperationType.FlatMap ||
         self._operation == OperationType.Filter ||
         self._operation == OperationType.Sample ||
         self._operation == OperationType.Repartition:
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._outputs[0]) :
                      Stream.SHUFFLE}
    elif self._operation == OperationType.Join:
      self._inputs = {}
      for parent in self._parents:
        self._inputs[GlobalStreamId(parent._stage_name, parent._outputs[0])] = JoinGrouping()
    elif self._operation == OperationType.ReduceByWindow:
      # Our parallelism is 1. So shuffle grouping will do
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._outputs[0]) :
                      Stream.SHUFFLE}
    elif self._operation == OperationType.ReduceByKeyAndWindow:
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._outputs[0]) :
                      ReduceGrouping()}
    elif self._operation == OperationType.Output:
      # TODO(skukarni):- is this correct
      self._inputs = {GlobalStreamId(self._parents[0]._stage_name, self._parents[0]._outputs[0]) :
                      Stream.SHUFFLE}
