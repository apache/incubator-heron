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
from abc import abstractmethod

from heronpy.api.topology import TopologyBuilder

from heronpy.dsl.dslboltbase import DslBoltBase

TimeWindow = namedtuple('TimeWindow', 'duration sliding_interval')

# pylint: disable=too-many-instance-attributes
class Streamlet(object):
  """A Streamlet is a (potentially unbounded) ordered collection of tuples
     Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from static data(such as
     csv files, HDFS files), or for that matter any other source. They are also created by
     transforming existing Streamlets using operations such as map/flat_map, etc.
  """
  @property
  def name(self):
    """The name of the Streamlet"""
    pass

  @name.setter
  def name(self, nm):
    pass

  @property
  def num_partitions(self):
    """The number of partitions"""
    pass

  @num_partitions.setter
  def num_partitions(self, n):
    pass

  def map(self, map_function):
    """Return a new Streamlet by applying map_function to each element of this Streamlet.
    """
    pass

  def flat_map(self, flatmap_function):
    """Return a new Streamlet by applying map_function to each element of this Streamlet
       and flattening the result
    """
    pass

  def filter(self, filter_function):
    """Return a new Streamlet containing only the elements that satisfy filter_function
    """
    pass

  def repartition(self, num_partitions):
    """Return a new Streamlet containing all elements of the this streamlet but having
    num_partitions partitions. Note that this is different from num_partitions(n) in
    that new streamlet will be created by the repartition call.
    """
    pass

  def repartition(self, num_partitions, repartition_function):
    """Same as above except to use repartition_function to choose
    where elements need to be sent. The repartiton_function is a function
    that for any particular element belonging to the current stream, decides
    which partitions(from 0 to num_partitions -1), it should route the element to.
    It could also return a list of partitions if it wants to send it to multiple
    partitions.
    """
    pass

  def clone(self, num_clones):
    """Return num_clones number of streamlets each containing all elements
    of the current streamlet
    """
    pass

  def reduce_by_window(self, window_config, reduce_function):
    """Return a new Streamlet in which each element of this Streamlet are collected
      over a window defined by window_config and then reduced using the reduce_function
      reduce_function takes two element at one time and reduces them to one element that
      is used in the subsequent operations.
    """
    pass

  def union(self, other_streamlet):
    """Returns a new Streamlet that consists of elements of both this and other_streamlet
    """
    pass

  def transform(self, transform_function):
    """Returns a  new Streamlet by applying the transform_function on each element of this
    streamlet. The transform_function is of the type TransformFunction.
    Before starting to cycle over the Streamlet, the open function of the transform_function is
    called. This allows the transform_function to do any kind of initialization/loading, etc.
    """
    pass

  def log(self):
    """Logs all elements of this streamlet. This returns nothing
    """
    pass

  def to_sink(self, consumer_function):
    """Calls consumer_function for each element of this streamlet. This function returns nothing
    """
    pass


  def join(self, join_streamlet, window_config, join_function):
    """Return a new Streamlet by joining join_streamlet with this streamlet
    """
    from heronpy.dsl.joinbolt import JoinStreamlet
    return JoinStreamlet(time_window, parents=[self, join_streamlet],
                         stage_name=stage_name, parallelism=parallelism)

  def reduce_by_key_and_window(self, time_window, reduce_function,
                               stage_name=None, parallelism=None):
    """Return a new Streamlet in which each (key, value) pair of this Streamlet are collected
       over the time_window and then reduced using the reduce_function
    """
    from heronpy.dsl.reducebykeyandwindowbolt import ReduceByKeyAndWindowStreamlet
    return ReduceByKeyAndWindowStreamlet(time_window, reduce_function,
                                         parents=[self],
                                         stage_name=stage_name, parallelism=parallelism)

  def run(self, name, config=None):
    """Runs the Streamlet. This is run as a Heron python topology under the name
       'name'. The config attached is passed on to this Heron topology
       Once submitted, run returns immediately
    """
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

  ##################################################################
  ### Internal functions
  ##################################################################

  # pylint: disable=protected-access
  def _build(self, bldr, stage_names):
    for parent in self._parents:
      parent._build(bldr, stage_names)
    if self._parallelism is None:
      self._parallelism = self._calculate_parallelism()
    if self._stage_name is None:
      self._stage_name = self._calculate_stage_name(stage_names)
    if self._stage_name in stage_names:
      raise RuntimeError("duplicated stage name %s" % self._stage_name)
    stage_names[self._stage_name] = 1
    self._build_this(bldr)
    return bldr

  @abstractmethod
  def _build_this(self, builder):
    """This is the method that's implemented by the operators.
    :type builder: TopologyBuilder
    :param builder: The operator adds in the current streamlet as a spout/bolt
    """
    raise RuntimeError("Streamlet's _build_this not implemented")

  # pylint: disable=protected-access
  @abstractmethod
  def _calculate_parallelism(self):
    """This is the method that's implemented by the operators with a default impl
    :return: The parallelism required for this operator
    """
    parallelism = 1
    for parent in self._parents:
      if parent._parallelism > parallelism:
        parallelism = parent._parallelism
    return parallelism

  @abstractmethod
  def _calculate_stage_name(self, existing_stage_names):
    """This is the method that's implemented by the operators to get the name of the Streamlet
    :return: The name of the operator
    """
    raise RuntimeError("Streamlet's calculate stage name not implemented")
