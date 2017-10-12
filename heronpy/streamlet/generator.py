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
'''generator.py: API for defining generic sources in python'''
from abc import abstractmethod

class Generator(object):
  """API for defining a generic source for Heron in the Python Streamlet API
  """

  @abstractmethod
  def setup(self, context):
    """Called when a task for this operator is initialized within a worker on the cluster

    It provides the operator with the environment in which the operator executes.
    This should be used to initialize any custom variables or connection to databases.

    :type context: :class:`Context`
    :param context: This object can be used to get information about this task's place within the
                    topology, including the task id and component id of this task, input and output
                    information, etc.
    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**
    """
    raise NotImplementedError("Generator not implementing setup() method.")

  @abstractmethod
  def get(self):
    """Generate the next element
    If there is nothing at the moment to generate, return None

    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**

    """
    raise NotImplementedError("Generator not implementing get() method.")
