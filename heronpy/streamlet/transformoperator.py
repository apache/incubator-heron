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
'''transformoperator.py: API for defining generic transformer in python'''
from abc import abstractmethod

class TransformOperator(object):
  """API for defining a generic transformer for Heron in the Python Streamlet API
  """

  @abstractmethod
  def setup(self, context):
    """Called when a task for this operator is initialized within a worker on the cluster

    It provides the operator with the environment in which the bolt executes.
    Note that ``__init__()`` should not be overriden for initialization of a bolt, as it is used
    internally by BaseBolt; instead, ``initialize()`` should be used to initialize any custom
    variables or connection to databases.

    :type context: :class:`Context`
    :param context: This object can be used to get information about this task's place within the
                    topology, including the task id and component id of this task, input and output
                    information, etc.
    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**
    """
    raise NotImplementedError("TransformOperator not implementing setup() method.")

  @abstractmethod
  def transform(self, tup):
    """Process a single tuple of input

    The Tuple object contains metadata on it about which component/stream/task it came from.
    To emit a tuple, call ``context.emit(tuple)``.

    **Must be implemented by a subclass, otherwise NotImplementedError is raised.**

    :type tup: :class:`Tuple`
    :param tup: Tuple to process
    """
    raise NotImplementedError("TransformOperator not implementing transform() method.")
