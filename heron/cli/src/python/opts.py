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
''' opts.py '''

################################################################################
# Global variable to store config map and verbosity
################################################################################
# pylint: disable=invalid-name,global-variable-not-assigned,global-statement
config_opts = dict()
verbose_flag = False
trace_execution_flag = False


################################################################################
def get_heron_config():
  '''
  Get config opts from the global variable
  :return:
  '''
  opt_list = []
  for (key, value) in config_opts.items():
    opt_list.append('%s=%s' % (key, value))

  all_opts = (','.join(opt_list)).replace(' ', '%%%%')
  return all_opts


################################################################################
def get_config(k):
  '''
  Get config opts from the config map
  :param k:
  :return:
  '''
  global config_opts
  if config_opts.has_key(k):
    return config_opts[k]
  return None


################################################################################
def set_config(k, v):
  '''
  Store a config opt in the config map
  :param k:
  :param v:
  :return:
  '''
  global config_opts
  config_opts[k] = v


################################################################################
def clear_config():
  '''
  Clear all the config in the config map
  :return:
  '''
  global config_opts
  config_opts = dict()


################################################################################
def set_trace_execution():
  '''
  Methods to get and set trace execution
  :return:
  '''
  global trace_execution_flag
  trace_execution_flag = True


def trace_execution():
  '''
  :return:
  '''
  global trace_execution_flag
  return trace_execution_flag
