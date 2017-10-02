# Copyright 2016 - Parsely, Inc. (d/b/a Parse.ly)
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
'''bolt.py: API for defining bolt in python'''
from abc import abstractmethod
from heronpy.api.bolt.base_bolt import BaseBolt

class Bolt(BaseBolt):
  """API for defining a bolt for Heron in Python

  Topology writers need to inherit this ``Bolt`` class to define their own custom bolt, by
  implementing ``initialize()`` and ``process()`` methods.
  """

  @abstractmethod
  def initialize(self, config, context):
    """Called when a task for this component is initialized within a worker on the cluster

    It provides the bolt with the environment in which the bolt executes.
    Note that ``__init__()`` should not be overriden for initialization of a bolt, as it is used
    internally by BaseBolt; instead, ``initialize()`` should be used to initialize any custom
    variables or connection to databases.

    :type config: dict
    :param config: The Heron configuration for this bolt. This is the configuration provided to
                   the topology merged in with cluster configuration on this machine.
                   Note that types of string values in the config have been automatically converted,
                   meaning that number strings and boolean strings are converted to the appropriate
                   types.
    :type context: :class:`TopologyContext`
    :param context: This object can be used to get information about this task's place within the
                    topology, including the task id and component id of this task, input and output
                    information, etc.
    """
    pass

  @abstractmethod
  def process(self, tup):
    """Process a single tuple of input

    The Tuple object contains metadata on it about which component/stream/task it came from.
    To emit a tuple, call ``self.emit(tuple)``.
    Note that tick tuples are not passed to this method, as the ``process_tick()`` method is
    responsible for processing them.

    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**

    :type tup: :class:`Tuple`
    :param tup: Tuple to process
    """
    raise NotImplementedError("Bolt not implementing process() method.")

  @abstractmethod
  def process_tick(self, tup):
    """Process special tick tuple

    It is compatible with StreamParse API.

    Tick tuples allow time-based behavior to be included in bolts. They will be sent to the bolts
    for which `topology.tick.tuple.freq.secs``
    (or, ``.api_constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS`` key) is set to an integer value, the
    number of seconds.

    Default behavior is to ignore tick tuples. This method should be overridden by subclasses
    if you want to react to timer events via tick tuples.

    :type tup: :class:`Tuple`
    :param tup: the tick tuple to be processed
    """
    pass
