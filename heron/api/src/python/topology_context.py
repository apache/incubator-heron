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
'''topology_context.py'''
from abc import abstractmethod

class TopologyContext(object):
  """Topology Context is the means for spouts/bolts to get information about
     the running topology. This file just is the interface to be used by spouts/bolts

  This is automatically created by Heron Instance and topology writers never need to create
  an instance by themselves.
  """
  @abstractmethod
  def get_task_id(self):
    """Gets the task id of this component
    :return: the task_id of this component
    """
    pass

  @abstractmethod
  def get_component_id(self):
    """Gets the component id of this component
    :return: the component_id of this component
    """
    pass

  @abstractmethod
  def get_cluster_config(self):
    """Returns the cluster config for this component
    Note that the returned config is auto-typed map: <str -> any Python object>.
    :return: the dict of key -> value
    """
    pass

  @abstractmethod
  def get_topology_name(self):
    """Returns the name of the topology
    :return: the name of the topology
    """
    pass

  @abstractmethod
  def register_metric(self, name, metric, time_bucket_in_sec):
    """Registers a new metric to this context
    :param name: The name of the metric
    :param metric: The IMetric that needs to be registered
    :param time_bucket_in_sec: The interval in seconds to do getValueAndReset
    """
    pass

  @abstractmethod
  def get_sources(self, component_id):
    """Returns the declared inputs to specified component
    :param component_id: The name of the component whose inputs we want

    :return: map <streamId namedtuple (same structure as protobuf msg) -> gtype>, or
             None if not found
    """
    pass

  def get_this_sources(self):
    """Returns the declared inputs to this component
    :return: map <streamId namedtuple (same structure as protobuf msg) -> gtype>, or
             None if not found
    """
    return self.get_sources(self.get_component_id())

  @abstractmethod
  def get_component_tasks(self, component_id):
    """Returns the task ids allocated for the given component id
    :param component_id: The name of the component whose task ids we want

    :return: list of task_ids or None if not found
    """
    pass

  @abstractmethod
  def add_task_hook(self, task_hook):
    """Registers a specified task hook to this context

    :type task_hook: ITaskHook
    :param task_hook: Implementation of ITaskHook
    """
    pass
