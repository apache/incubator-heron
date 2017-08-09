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

from heron.api.src.python import TopologyBuilder

from heron.dsl.src.python.operation import OperationType

TimeWindow = namedtuple('TimeWindow', 'duration sliding_interval')

# pylint: disable=too-many-instance-attributes
class Streamlet(object):
  """A Streamlet is a (potentially unbounded) ordered collection of tuples
  """
  def __init__(self, parents, operation=None, stage_name=None,
               parallelism=None, inputs=None):
    """
    """
    if operation is None:
      raise RuntimeError("Streamlet's operation cannot be None")
    if not OperationType.valid(operation):
      raise RuntimeError("Streamlet's operation must be of type OperationType")
    if not isinstance(parents, list):
      raise RuntimeError("Streamlet's parents have to be a list")
    self._parents = parents
    self._operation = operation
    self._stage_name = stage_name
    self._parallelism = parallelism
    self._inputs = inputs
    self._output = 'output'

  def map(self, map_function, stage_name=None, parallelism=None):
    from heron.dsl.src.python.mapbolt import MapStreamlet
    return MapStreamlet(map_function, parents=[self], stage_name=stage_name,
                        parallelism=parallelism)

  def flatMap(self, flatmap_function, stage_name=None, parallelism=None):
    from heron.dsl.src.python.flatmapbolt import FlatMapStreamlet
    return FlatMapStreamlet(flatmap_function, parents=[self], stage_name=stage_name,
                            parallelism=parallelism)

  def filter(self, filter_function, stage_name=None, parallelism=None):
    from heron.dsl.src.python.filterbolt import FilterStreamlet
    return FilterStreamlet(filter_function, parents=[self], stage_name=stage_name,
                           parallelism=parallelism)

  def sample(self, sample_fraction, stage_name=None, parallelism=None):
    from heron.dsl.src.python.samplebolt import SampleStreamlet
    return SampleStreamlet(sample_fraction, parents=[self], stage_name=stage_name,
                           parallelism=parallelism)

  def repartition(self, parallelism, stage_name=None):
    from heron.dsl.src.python.repartitionbolt import RepartitionStreamlet
    return RepartitionStreamlet(parallelism, parents=[self], stage_name=stage_name)

  def join(self, join_streamlet, time_window, stage_name=None, parallelism=None):
    from heron.dsl.src.python.joinbolt import JoinStreamlet
    return JoinStreamlet(time_window, parents=[self, join_streamlet],
                         operation=OperationType.Join,
                         stage_name=stage_name, parallelism=parallelism)

  def reduceByWindow(self, time_window, reduce_function, stage_name=None):
    from heron.dsl.src.python.reducebykeyandwindowbolt import ReduceByKeyAndWindowStreamlet
    return ReduceByKeyAndWindowStreamlet(time_window, reduce_function,
                                         parents=[self],
                                         stage_name=stage_name, parallelism=1)

  def reduceByKeyAndWindow(self, time_window, reduce_function, stage_name=None, parallelism=None):
    from heron.dsl.src.python.reducebykeyandwindowbolt import ReduceByKeyAndWindowStreamlet
    return ReduceByKeyAndWindowStreamlet(time_window, reduce_function,
                                         parents=[self],
                                         stage_name=stage_name, parallelism=parallelism)

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
    self._inputs = self._calculate_inputs()
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
    raise RuntimeError("Streamlet's _build_this not implemented")

  # pylint: disable=protected-access
  @abstractmethod
  def _calculate_parallelism(self):
    parallelism = 1
    for parent in self._parents:
      if parent._parallelism > parallelism:
        parallelism = parent._parallelism
    return parallelism

  @abstractmethod
  def _calculate_inputs(self):
    raise RuntimeError("Streamlet's calculate inputs not implemented")

  @abstractmethod
  def _calculate_stage_name(self, existing_stage_names):
    raise RuntimeError("Streamlet's calculate stage name not implemented")
