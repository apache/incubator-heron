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
'''base_component.py'''

class BaseComponent(object):
  """Base component for Heron spout and bolt"""
  def __init__(self, delegate):
    """Initializes BaseComponent

    :param delegate: SpoutInstance or BoltInstance
    """
    self.delegate = delegate
    self.logger = self.delegate.logger

  def log(self, message, level=None):
    """Log message, optionally providing a logging level

    :type message: str
    :param message: the log message to send
    :type level: str
    :param level: the logging level,
                  one of: trace (=debug), debug, info, warn or error (default: info)
    """
    self.delegate.log(message, level)
