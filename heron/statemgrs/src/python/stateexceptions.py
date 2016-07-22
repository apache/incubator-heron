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
''' stateexceptions.py '''

class StateException(Exception):
  """ state exception """
  EX_TYPE_NO_NODE_ERROR = 1
  EX_TYPE_NODE_EXISTS_ERROR = 2
  EX_TYPE_NOT_EMPTY_ERROR = 3
  EX_TYPE_ZOOKEEPER_ERROR = 4
  EX_TYPE_PROTOBUF_ERROR = 5

  def __init__(self, message, exType):
    # Call the Exception constructor with
    # the message
    Exception.__init__(self, message)

    self.message = message
    self.exType = exType
