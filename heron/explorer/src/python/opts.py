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

################################################################################
# Global variable to store config map and verbosity
################################################################################
config_opts = dict()
verbose_flag = False
trace_execution_flag = False

################################################################################
# Get config opts from the global variable
################################################################################
def get_heron_config():
  opt_list = []
  for (k, v) in config_opts.items():
    opt_list.append('%s=%s' % (k, v))

  all_opts = '-Dheron.options=' + (','.join(opt_list)).replace(' ', '%%%%')
  return all_opts

################################################################################
# Get config opts from the config map
################################################################################
def get_config(k):
  global config_opts
  if config_opts.has_key(k):
    return config_opts[k]
  return None

################################################################################
# Store a config opt in the config map
################################################################################
def set_config(k, v):
  global config_opts
  config_opts[k] = v

################################################################################
# Clear all the config in the config map
################################################################################
def clear_config():
  global config_opts
  config_opts = dict()

################################################################################
# Methods to get and set verbose levels
################################################################################
def set_verbose():
  global verbose_flag
  verbose_flag = True

def verbose():
  global verbose_flag
  return verbose_flag

################################################################################
# Methods to get and set trace execution
################################################################################
def set_trace_execution():
  global trace_execution_flag
  trace_execution_flag = True
  set_verbose()

def trace_execution():
  global trace_execution_flag
  return trace_execution_flag
