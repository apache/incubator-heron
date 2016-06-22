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

import os, fnmatch

import heron.cli.src.python.utils as utils

################################################################################
# Get the topology jars - TODO, make the jars independent version free
################################################################################
def pick(dirname, pattern):
  file_list = fnmatch.filter(os.listdir(dirname), pattern)
  return file_list[0] if file_list else None
    
################################################################################
# Get the topology jars - TODO, make the jars independent version free
################################################################################
def topology_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "third_party", "*")
  ]
  return jars

################################################################################
# Get the scheduler jars
################################################################################
def scheduler_jars():
  jars = [
       os.path.join(utils.get_heron_lib_dir(), "scheduler", "*")
  ]
  return jars

################################################################################
# Get the uploader jars
################################################################################
def uploader_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "uploader", "*")
  ]
  return jars

################################################################################
# Get the statemgr jars
################################################################################
def statemgr_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "statemgr", "*")
  ]
  return jars

################################################################################
# Get the packing algorithm jars
################################################################################
def packing_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "packing", "*")
  ]
  return jars
