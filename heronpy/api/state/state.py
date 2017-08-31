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
'''state.py'''
from abc import abstractmethod

class State(object):
  """State represents the state interface as seen by stateful bolts and spouts.
  In Heron, state gives a notional Key/Value interface along with the
  ability to iterate over the key/values
  """
  @abstractmethod
  def put(self, key, value):
    """Puts {key, value} pair into the state
    :param key: The key to get back the value
    :param value: The value associated with the key
    """
    pass

  @abstractmethod
  def get(self, key):
    """Gets the value corresponding to a key
    :param key: The key whose value we want back
    :return: The value associated with the key
    """
    pass

  @abstractmethod
  def enumerate(self):
    """Allows one to enumerate over the state.
    :return: The enumerate object
    """
    pass

  @abstractmethod
  def clear(self):
    """Clears the state to empty state
    """
    pass

class HashMapState(State):
  """HashMapState represents default implementation of the State interface
  """
  def __init__(self):
    self._dict = {}

  def put(self, k, v):
    self._dict[k] = v

  def get(self, k):
    if k in self._dict:
      return self._dict[k]
    else:
      return None

  def enumerate(self):
    return enumerate(self._dict)

  def clear(self):
    self._dict.clear()

  def __str__(self):
    return str(self._dict)
