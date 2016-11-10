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
#!/usr/bin/env python2.7

import logging

""" Classes to represent the success or failure of an integration test """

class TestFailure(Exception):
  def __init__(self, message, error=None):
    Exception.__init__(self, message, error)
    if error:
      logging.error("%s :: %s", message, str(error))
    else:
      logging.error(message)

class TestSuccess(object):
  def __init__(self, message=None):
    if message:
      logging.info(message)
