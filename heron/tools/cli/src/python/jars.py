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
''' jars.py '''
import os
import fnmatch

import heron.tools.common.src.python.utils.config as config


def pick(dirname, pattern):
  '''
  Get the topology jars
  :param dirname:
  :param pattern:
  :return:
  '''
  file_list = fnmatch.filter(os.listdir(dirname), pattern)
  return file_list[0] if file_list else None


def topology_jars():
  '''
  Get the topology jars
  :return:
  '''
  jars = [
      os.path.join(config.get_heron_lib_dir(), "third_party", "*")
  ]
  return jars


def scheduler_jars():
  '''
  Get the scheduler jars
  :return:
  '''
  jars = [
      os.path.join(config.get_heron_lib_dir(), "scheduler", "*")
  ]
  return jars


def uploader_jars():
  '''
  Get the uploader jars
  :return:
  '''
  jars = [
      os.path.join(config.get_heron_lib_dir(), "uploader", "*")
  ]
  return jars


def statemgr_jars():
  '''
  Get the statemgr jars
  :return:
  '''
  jars = [
      os.path.join(config.get_heron_lib_dir(), "statemgr", "*")
  ]
  return jars


def packing_jars():
  '''
  Get the packing algorithm jars
  :return:
  '''
  jars = [
      os.path.join(config.get_heron_lib_dir(), "packing", "*")
  ]
  return jars
