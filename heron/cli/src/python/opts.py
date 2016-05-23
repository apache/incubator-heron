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
CONFIG_OPTS = dict()
VERBOSE_FLAG = False
TRACE_EXECUTION_FLAG = False


################################################################################
# Get config opts from the global variable
################################################################################
def get_heron_config():
    '''
    :return:
    '''
    opt_list = []
    for (key, value) in CONFIG_OPTS.items():
        opt_list.append('%s=%s' % (key, value))

    all_opts = '-Dheron.options=' + (','.join(opt_list)).replace(' ', '%%%%')
    return all_opts


################################################################################
# Get config opts from the config map
################################################################################
def get_config(k):
    '''
    :param k:
    :return:
    '''
    global CONFIG_OPTS
    if CONFIG_OPTS.has_key(k):
        return CONFIG_OPTS[k]
    return None


################################################################################
# Store a config opt in the config map
################################################################################
def set_config(key, value):
    '''
    :param key:
    :param value:
    :return:
    '''
    global CONFIG_OPTS
    CONFIG_OPTS[key] = value


################################################################################
# Clear all the config in the config map
################################################################################
def clear_config():
    '''
    :return:
    '''
    global CONFIG_OPTS
    CONFIG_OPTS = dict()


################################################################################
# Methods to get and set verbose levels
################################################################################
def set_verbose():
    '''
    :return:
    '''
    global VERBOSE_FLAG
    VERBOSE_FLAG = True


def verbose():
    '''
    :return:
    '''
    global VERBOSE_FLAG
    return VERBOSE_FLAG


################################################################################
# Methods to get and set trace execution
################################################################################
def set_trace_execution():
    '''
    :return:
    '''
    global TRACE_EXECUTION_FLAG
    TRACE_EXECUTION_FLAG = True
    set_verbose()


def trace_execution():
    '''
    :return:
    '''
    global TRACE_EXECUTION_FLAG
    return TRACE_EXECUTION_FLAG
