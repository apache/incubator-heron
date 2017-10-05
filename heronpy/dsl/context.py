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
'''context.py: module for defining context'''

from abc import abstractmethod

class Context(object):
  """Context is the information available at runtime for operators like transform.
     It contains basic things like config, runtime information like task,
     the stream that it is operating on, ProcessState, etc.
  """

  @abstractmethod
  def get_task_id(self):
    """Fetches the task id of the current instance of the operator
    """
    pass

  @abstractmethod
  def get_config(self):
    """Fetches the config of the computation
    """
    pass

  @abstractmethod
  def get_stream_name(self):
    """Fetches the stream name that we are operating on
    """
    pass

  @abstractmethod
  def get_num_partitions(self):
    """Fetches the number of partitions of the stream we are operating on
    """
    pass

  def get_partition_index(self):
    """Fetches the partition of the stream that we are operating on
    """
    pass

  @abstractmethod
  def get_state(self):
    """The state where components can store any of their local state
    """
    pass

  @abstractmethod
  def emit(self, values):
    """Emits the values in the output stream
    """
    pass
