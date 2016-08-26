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
'''classpath.py: util functions for verifying a java class path, mainly for heron-cli'''

import os

from heron.common.src.python.utils.log import Log

def valid_path(path):
  '''
  Check if an entry in the class path exists as either a directory or a file
  '''
  # check if the class path directory exists
  if path.endswith('*'):
    Log.debug('Checking class path directory: %s', path[-1])
    if os.path.isdir(path[-1]):
      return True
    return False

  # check if the class path file exists
  Log.debug('Checking class path file: %s', path)
  if os.path.isfile(path):
    return True
  return False


def valid_java_classpath(classpath):
  '''
  Given a java classpath, check whether the path entries are valid or not
  '''
  paths = classpath.split(':')
  for path_entry in paths:
    if not valid_path(path_entry.strip()):
      return False
  return True
