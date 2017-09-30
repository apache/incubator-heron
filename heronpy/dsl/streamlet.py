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

# pylint: disable=too-many-instance-attributes
class Streamlet(object):
  """A Streamlet is a (potentially unbounded) ordered collection of tuples
     Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from static data(such as
     csv files, HDFS files), or for that matter any other source. They are also created by
     transforming existing Streamlets using operations such as map/flat_map, etc.
  """
  def __init__(self):
    """
    """
    self._children = None
    self._name = None
    self._num_partitions = 1
    self._children = []
    self._built = False
    self._output = DslBoltBase.outputs[0].stream_id

  def set_name(self, name):
    """Sets the name of the Streamlet"""
    self._name = name
    return self

  def get_name(self):
    return self._name

  def set_num_partitions(self, num_partitions):
    """Sets the number of partitions"""
    self._num_partitions = num_partitions
    return self

  def get_num_partitions(self):
    return self._num_partitions

  def map(self, map_function):
    """Return a new Streamlet by applying map_function to each element of this Streamlet.
    """
    from heronpy.dsl.mapbolt import MapStreamlet
    map_streamlet = MapStreamlet(map_function, self)
    self._add_child(map_streamlet)
    return map_streamlet

  def flat_map(self, flatmap_function):
    """Return a new Streamlet by applying map_function to each element of this Streamlet
       and flattening the result
    """
    from heronpy.dsl.flatmapbolt import FlatMapStreamlet
    fm_streamlet = FlatMapStreamlet(flatmap_function, self)
    self._add_child(fm_streamlet)
    return fm_streamlet

  def filter(self, filter_function):
    """Return a new Streamlet containing only the elements that satisfy filter_function
    """
    from heronpy.dsl.filterbolt import FilterStreamlet
    filter_streamlet = FilterStreamlet(filter_function, self)
    self._add_child(filter_streamlet)
    return filter_streamlet

  def repartition(self, num_partitions):
    """Return a new Streamlet containing all elements of the this streamlet but having
    num_partitions partitions. Note that this is different from num_partitions(n) in
    that new streamlet will be created by the repartition call.
    """
    from heronpy.dsl.repartitionbolt import RepartitionStreamlet
    repartition_streamlet = RepartitionStreamlet(parallelism, self)
    self._add_child(repartition_streamlet)
    return repartition_streamlet

  def repartition(self, num_partitions, repartition_function):
    """Same as above except to use repartition_function to choose
    where elements need to be sent. The repartiton_function is a function
    that for any particular element belonging to the current stream, decides
    which partitions(from 0 to num_partitions -1), it should route the element to.
    It could also return a list of partitions if it wants to send it to multiple
    partitions.
    """
    from heronpy.dsl.repartitionbolt import RepartitionStreamlet
    repartition_streamlet = RepartitionStreamlet(num_partitions, repartition_function, self)
    self._add_child(repartition_streamlet)
    return repartition_streamlet

  def clone(self, num_clones):
    """Return num_clones number of streamlets each containing all elements
    of the current streamlet
    """
    retval = []
    for i in xrange(num_clones):
      retval.append(self.repartition(self.get_num_partitions()))
    return retval

  def reduce_by_window(self, window_config, reduce_function):
    """Return a new Streamlet in which each element of this Streamlet are collected
      over a window defined by window_config and then reduced using the reduce_function
      reduce_function takes two element at one time and reduces them to one element that
      is used in the subsequent operations.
    """
    from heronpy.dsl.reducebywindowbolt import ReduceByWindowBolt
    reduce_streamlet = ReduceByWindowBolt(window_config, reduce_function, self)
    self._add_child(reduce_streamlet)
    return reduce_streamlet

  def union(self, other_streamlet):
    """Returns a new Streamlet that consists of elements of both this and other_streamlet
    """
    from heronpy.dsl.unionbolt import UnionBolt
    union_streamlet = UnionBolt(self, other_streamlet)
    self._add_child(union_streamlet)
    other_streamlet._add_child(union_streamlet)
    return union_streamlet

  def transform(self, transform_function):
    """Returns a  new Streamlet by applying the transform_function on each element of this
    streamlet. The transform_function is of the type TransformFunction.
    Before starting to cycle over the Streamlet, the open function of the transform_function is
    called. This allows the transform_function to do any kind of initialization/loading, etc.
    """
    from heronpy.dsl.transformbolt import TransformStreamlet
    transform_streamlet = TransformStreamlet(transform_function, self)
    self._add_child(transform_streamlet)
    return transform_streamlet

  def log(self):
    """Logs all elements of this streamlet. This returns nothing
    """
    from heronpy.dsl.logbolt import LogStreamlet
    log_streamlet = LogStreamlet(self)
    self._add_child(log_streamlet)
    return

  def to_sink(self, consumer_function):
    """Calls consumer_function for each element of this streamlet. This function returns nothing
    """
    from heronpy.dsl.sinkbolt import SinkStreamlet
    sink_streamlet = SinkStreamlet(consumer_function, self)
    self._add_child(sink_streamlet)
    return

  def join(self, join_streamlet, window_config, join_function):
    """Return a new Streamlet by joining join_streamlet with this streamlet
    """
    from heronpy.dsl.joinbolt import JoinStreamlet
    join_streamlet = JoinStreamlet(JoinStreamlet.INNER, window_config,
                                   join_function, self, join_streamlet)
    self._add_child(join_streamlet)
    join_streamlet._add_child(join_streamlet)
    return join_streamlet

  def outer_join(self, join_streamlet, window_config, join_function):
    """Return a new Streamlet by outer joining join_streamlet with this streamlet
    """
    from heronpy.dsl.joinbolt import JoinStreamlet
    join_streamlet = JoinStreamlet(JoinStreamlet.OUTER, window_config,
                                   join_function, self, join_streamlet)
    self._add_child(join_streamlet)
    join_streamlet._add_child(join_streamlet)
    return join_streamlet

  def left_join(self, join_streamlet, window_config, join_function):
    """Return a new Streamlet by left joining join_streamlet with this streamlet
    """
    from heronpy.dsl.joinbolt import JoinStreamlet
    join_streamlet = JoinStreamlet(JoinStreamlet.LEFT, window_config,
                                   join_function, self, join_streamlet)
    self._add_child(join_streamlet)
    join_streamlet._add_child(join_streamlet)
    return join_streamlet

  def reduce_by_key_and_window(self, window_config, reduce_function):
    """Return a new Streamlet in which each (key, value) pair of this Streamlet are collected
       over the time_window and then reduced using the reduce_function
    """
    from heronpy.dsl.reducebykeyandwindowbolt import ReduceByKeyAndWindowStreamlet
    reduce_streamlet = ReduceByKeyAndWindowStreamlet(window_config, reduce_function, self)
    self._add_child(reduce_streamlet)
    return reduce_streamlet

  ##################################################################
  ### Internal functions
  ##################################################################

  # pylint: disable=protected-access
  def _build(self, bldr, stage_names):
    if self._built:
      raise RuntimeError("Logic error while building")
    if self.build_this(bldr, stage_names):
      self._built = True
      for children in self._children:
        children.build(bldr, stage_names)

  @abstractmethod
  def _build_this(self, builder, stage_names):
    """This is the method that's implemented by the operators.
    :type builder: TopologyBuilder
    :param builder: The operator adds in the current streamlet as a spout/bolt
    """
    raise RuntimeError("Streamlet's _build_this not implemented")

  def _add_child(self, child):
    self._children.append(child)

  def _default_stage_name_calculator(self, prefix, existing_stage_names):
    """This is the method that's implemented by the operators to get the name of the Streamlet
    :return: The name of the operator
    """
    index = 1
    calculated_name = ""
    while True:
      calculated_name = prefix + "-" + str(index)
      if calculated_name in existing_stage_names:
        return calculated_name
      index = index + 1
    return "Should Never Got Here"
