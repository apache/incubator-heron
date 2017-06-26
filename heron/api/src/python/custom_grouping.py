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
'''custom_grouping.py: interface module for custom grouping'''
from abc import abstractmethod

class ICustomGrouping(object):
  '''Interface for custom grouping class'''

  @abstractmethod
  def prepare(self, context, component, stream, target_tasks):
    """Tells the stream grouping at runtime the tasks in the target bolt

    This information should be used in ``choose_tasks()`` to determine the target tasks.

    :param context: topology-worker context
    :param component: source component name
    :param stream: stream id used for this grouping
    :type target_tasks: list of int
    :param target_tasks: list of target task ids
    """
    pass

  @abstractmethod
  def choose_tasks(self, values):
    """Implements a custom stream grouping

    :param values: the values to group on
    :rtype: list of int
    :return: list of task ids to which these values are emitted
    """
    pass
