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
'''bolt.py: API for defining bolt in python'''
from abc import abstractmethod
from heronpy.api.spout.base_spout import BaseSpout

class Spout(BaseSpout):
  """API for defining a spout for Heron in Python

  Topology writers need to inherit this ``Spout`` class to define their own custom spout, by
  implementing ``initialize()``, ``next_tuple()``, ``ack()`` and ``fail()`` methods.
  In addition, ``close()``, ``activate()`` and ``deactivate()`` are available to be implemented.
  """

  @abstractmethod
  def initialize(self, config, context):
    """Called when a task for this component is initialized within a worker on the cluster

    It provides the spout with the environment in which the spout executes. Note that
    you should NOT override ``__init__()`` for initialization of your spout, as it is
    used internally by Heron Instance; instead, you should use this method to initialize
    any custom instance variables or connections to data sources.

    *Should be implemented by a subclass.*

    :type config: dict
    :param config: The Heron configuration for this bolt. This is the configuration provided to
                   the topology merged in with cluster configuration on this machine.
                   Note that types of string values in the config have been automatically converted,
                   meaning that number strings and boolean strings are converted to appropriate
                   types.
    :type context: :class:`TopologyContext`
    :param context: This object can be used to get information about this task's place within the
                    topology, including the task id and component id of this task, input and output
                    information, etc.
    """
    pass

  @abstractmethod
  def close(self):
    """Called when this spout is going to be shutdown

    There is no guarantee that close() will be called.
    """
    pass

  @abstractmethod
  def next_tuple(self):
    """When this method is called, Heron is requesting that the Spout emit tuples

    It is compatible with StreamParse API.

    This method should be non-blocking, so if the Spout has no tuples to emit,
    this method should return; next_tuple(), ack(), and fail() are all called in a tight
    loop in a single thread in the spout task. WHen there are no tuples to emit, it is
    courteous to have next_tuple sleep for a short amount of time (like a single millisecond)
    so as not to waste too much CPU.

    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**
    """
    raise NotImplementedError("Spout not implementing next_tuple() method")

  @abstractmethod
  def ack(self, tup_id):
    """Determine that the tuple emitted by this spout with the tup_id has been fully processed

    It is compatible with StreamParse API.

    Heron has determined that the tuple emitted by this spout with the tup_id identifier
    has been fully processed. Typically, an implementation of this method will take that
    message off the queue and prevent it from being replayed.

    *Should be implemented by a subclass.*

    :param tup_id: the ID of the HeronTuple that has been fully acknowledged.
    """
    pass

  @abstractmethod
  def fail(self, tup_id):
    """Determine that the tuple emitted by this spout with the tup_id has failed to be processed

    It is compatible with StreamParse API.

    The tuple emitted by this spout with the tup_id identifier has failed to be
    fully processed. Typically, an implementation of this method will put that
    message back on the queue to be replayed at a later time.

    *Should be implemented by a subclass.*

    :param tup_id: the ID of the HeronTuple that has failed either due to a bolt calling ``fail()``
                   or timeout
    """
    pass

  @abstractmethod
  def activate(self):
    """Called when a spout has been activated out of a deactivated mode

    next_tuple() will be called on this spout soon. A spout can become activated
    after having been deactivated when the topology is manipulated using the
    `heron` client.
    """
    pass

  @abstractmethod
  def deactivate(self):
    """Called when a spout has been deactivated

    next_tuple() will not be called while a spout is deactivated.
    The spout may or may not be reactivated in the future.
    """
    pass
