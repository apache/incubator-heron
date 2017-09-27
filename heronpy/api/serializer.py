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
'''serializer.py: common python serializer for heron'''
from abc import abstractmethod

try:
  import cPickle as pickle
except:
  import pickle

import heronpy.api.cloudpickle as cloudpickle

class IHeronSerializer(object):
  """Serializer interface for Heron"""
  @abstractmethod
  def initialize(self, config):
    """Initializes the serializer"""
    pass

  @abstractmethod
  def serialize(self, obj):
    """Serialize an object

    :param obj: The object to be serialized
    :returns: Serialized object as byte string
    """
    pass

  @abstractmethod
  def deserialize(self, input_str):
    """Deserialize an object

    :param input_str: Serialized object as byte string
    :returns: Deserialized object
    """
    pass

class PythonSerializer(IHeronSerializer):
  """Default serializer"""
  def initialize(self, config=None):
    pass

  def serialize(self, obj):
    return cloudpickle.dumps(obj)

  def deserialize(self, input_str):
    return pickle.loads(input_str)

default_serializer = PythonSerializer()
