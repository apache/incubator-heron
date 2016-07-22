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

''' heron-executor '''

import atexit
import datetime
import os
import sys
import signal
import time

from heron.executor.src.python.heron_executor_core import *

def print_usage():
  '''print usage'''
  print (
      "./heron-executor <shardid> <topname> <topid> <topdefnfile> "
      " <instance_distribution> <zknode> <zkroot> <tmaster_binary> <stmgr_binary> "
      " <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath> "
      " <master_port> <tmaster_controller_port> <tmaster_stats_port> <heron_internals_config_file> "
      " <component_rammap> <component_jvm_opts_in_base64> <pkg_type> <topology_jar_file>"
      " <heron_java_home> <shell-port> <heron_shell_binary> <metricsmgr_port>"
      " <cluster> <role> <environ> <instance_classpath> <metrics_sinks_config_file> "
      " <scheduler_classpath> <scheduler_port>")

def do_print(statement):
  """ print message """
  timestr = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
  print "%s: %s" % (timestr, statement)
  sys.stdout.flush()

# This is a separate class from heron-executor-core.py for backward compatibility. Files with
# underscores can't be imported (which we need to do for unit tests), everything besides the main
# method was moved out of this file and into heron-executor-core.py. Instead we could have renamed
# this file to heron_executor.py but that change is more disruptive.
def main():
  ''' main '''
  if len(sys.argv) != 32:
    print_usage()
    sys.exit(1)
  # pylint: disable=undefined-variable
  executor = HeronExecutor(sys.argv)
  executor.prepareLaunch()
  executor.register_packing_plan_watcher(executor)
  executor.monitor_processes()

# pylint: disable=unused-argument
def signal_handler(signal_to_handle, frame):
  ''' signal handler '''
  # We would do nothing here but just exit
  # Just catch the SIGTERM and then cleanup(), registered with atexit, would invoke
  sys.exit(signal_to_handle)

def setup():
  ''' setup '''
  # Redirect stdout and stderr to files in append mode
  # The filename format is heron-executor.stdxxx
  sys.stdout = open('heron-executor.stdout', 'a')
  sys.stderr = open('heron-executor.stderr', 'a')

  do_print('Set up process group; executor becomes leader')
  os.setpgrp() # create new process group, become its leader

  do_print('Register the SIGTERM signal handler')
  signal.signal(signal.SIGTERM, signal_handler)

  do_print('Register the atexit clean up')
  atexit.register(cleanup)

def cleanup():
  """Handler to trigger when receiving the SIGTERM signal
  Do cleanup inside this method, including:
  1. Terminate all children processes
  """
  do_print('Executor terminated; exiting all process in executor.')
  # We would not wait or check whether process spawned dead or not
  os.killpg(0, signal.SIGTERM)

if __name__ == "__main__":
  setup()
  main()
