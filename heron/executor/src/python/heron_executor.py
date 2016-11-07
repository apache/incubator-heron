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
""" The Heron executor is a process that runs on a container and is responsible for starting and
monitoring the processes of the topology and it's support services."""
import argparse
import atexit
import base64
import json
import os
import random
import signal
import string
import subprocess
import sys
import stat
import threading
import time
import yaml

from functools import partial



from heron.common.src.python.utils import log
# pylint: disable=unused-import
from heron.proto.packing_plan_pb2 import PackingPlan
from heron.statemgrs.src.python import statemanagerfactory
from heron.statemgrs.src.python import configloader
from heron.statemgrs.src.python.config import Config as StateMgrConfig

Log = log.Log

def print_usage():
  print (
      "Usage: ./heron-executor <shardid> <topname> <topid> <topdefnfile>"
      " <zknode> <zkroot> <tmaster_binary> <stmgr_binary>"
      " <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath>"
      " <master_port> <tmaster_controller_port> <tmaster_stats_port> <heron_internals_config_file>"
      " <component_rammap> <component_jvm_opts_in_base64> <pkg_type> <topology_bin_file>"
      " <heron_java_home> <shell-port> <heron_shell_binary> <metricsmgr_port>"
      " <cluster> <role> <environ> <instance_classpath> <metrics_sinks_config_file>"
      " <scheduler_classpath> <scheduler_port> <python_instance_binary>")

def id_map(prefix, container_plans, add_zero_id=False):
  ids = {}
  if add_zero_id:
    ids[0] = "%s-0" % prefix

  for container_plan in container_plans:
    ids[container_plan.id] = "%s-%d" % (prefix, container_plan.id)
  return ids

def stmgr_map(container_plans):
  return id_map("stmgr", container_plans)

def metricsmgr_map(container_plans):
  return id_map("metricsmgr", container_plans, True)

def heron_shell_map(container_plans):
  return id_map("heron-shell", container_plans, True)

def get_heron_executor_process_name(shard_id):
  return 'heron-executor-%d' % shard_id

def get_process_pid_filename(process_name):
  return '%s.pid' % process_name

def get_tmp_filename():
  return '%s.heron.tmp' % (''.join(random.choice(string.ascii_uppercase) for i in range(12)))

def atomic_write_file(path, content):
  """
  file.write(...) is not atomic.
  We write to a tmp file and then rename to target path since rename is atomic.
  We do this to avoid the content of file is dirty read/partially read by others.
  """
  # Write to a randomly tmp file
  tmp_file = get_tmp_filename()
  with open(tmp_file, 'w') as f:
    f.write(content)
    # make sure that all data is on disk
    f.flush()
    os.fsync(f.fileno())

  # Rename the tmp file
  os.rename(tmp_file, path)

def log_pid_for_process(process_name, pid):
  filename = get_process_pid_filename(process_name)
  Log.info('Logging pid %d to file %s' %(pid, filename))
  atomic_write_file(filename, str(pid))

class ProcessInfo(object):
  def __init__(self, process, name, command, attempts=1):
    """
    Container for info related to a running process
    :param process: the process POpen object
    :param name: the logical (i.e., unique) name of the process
    :param command: an array of strings comprising the command and it's args
    :param attempts: how many times the command has been run (defaults to 1)
    """
    self.process = process
    self.pid = process.pid
    self.name = name
    self.command = command
    self.command_str = ' '.join(command) # convenience for unit tests
    self.attempts = attempts

  def increment_attempts(self):
    self.attempts += 1
    return self

# pylint: disable=too-many-instance-attributes
class HeronExecutor(object):
  """ Heron executor is a class that is responsible for running each of the process on a given
  container. Based on the container id and the instance distribution, it determines if the container
  is a master node or a worker node and it starts processes accordingly."""
  def __init__(self, args, shell_env):
    parsed_args = self.parse_args(args)

    self.shell_env = shell_env
    self.max_runs = 100
    self.interval_between_runs = 10
    self.shard = parsed_args.shard
    self.topology_name = parsed_args.topology_name
    self.topology_id = parsed_args.topology_id
    self.topology_defn_file = parsed_args.topology_defn_file
    self.zknode = parsed_args.zknode
    self.zkroot = parsed_args.zkroot
    self.tmaster_binary = parsed_args.tmaster_binary
    self.stmgr_binary = parsed_args.stmgr_binary
    self.metricsmgr_classpath = parsed_args.metricsmgr_classpath
    self.instance_jvm_opts =\
        base64.b64decode(parsed_args.instance_jvm_opts.lstrip('"').
                         rstrip('"').replace('&equals;', '='))
    self.classpath = parsed_args.classpath
    self.master_port = parsed_args.master_port
    self.tmaster_controller_port = parsed_args.tmaster_controller_port
    self.tmaster_stats_port = parsed_args.tmaster_stats_port
    self.heron_internals_config_file = parsed_args.heron_internals_config_file
    self.component_rammap =\
        map(lambda x: {x.split(':')[0]:
                           int(x.split(':')[1])}, parsed_args.component_rammap.split(','))
    self.component_rammap =\
        reduce(lambda x, y: dict(x.items() + y.items()), self.component_rammap)

    # component_jvm_opts_in_base64 itself is a base64-encoding-json-map, which is appended with
    # " at the start and end. It also escapes "=" to "&equals" due to aurora limitation
    # And the json is a map from base64-encoding-component-name to base64-encoding-jvm-options
    self.component_jvm_opts = {}
    # First we need to decode the base64 string back to a json map string
    component_jvm_opts_in_json =\
        base64.b64decode(parsed_args.component_jvm_opts_in_base64.
                         lstrip('"').rstrip('"').replace('&equals;', '='))
    if component_jvm_opts_in_json != "":
      for (k, v) in json.loads(component_jvm_opts_in_json).items():
        # In json, the component name and jvm options are still in base64 encoding
        self.component_jvm_opts[base64.b64decode(k)] = base64.b64decode(v)

    self.pkg_type = parsed_args.pkg_type
    self.topology_bin_file = parsed_args.topology_bin_file
    self.heron_java_home = parsed_args.heron_java_home
    self.shell_port = parsed_args.shell_port
    self.heron_shell_binary = parsed_args.heron_shell_binary
    self.metricsmgr_port = parsed_args.metricsmgr_port
    self.cluster = parsed_args.cluster
    self.role = parsed_args.role
    self.environ = parsed_args.environ
    self.instance_classpath = parsed_args.instance_classpath
    self.metrics_sinks_config_file = parsed_args.metrics_sinks_config_file
    self.scheduler_classpath = parsed_args.scheduler_classpath
    self.scheduler_port = parsed_args.scheduler_port
    self.python_instance_binary = parsed_args.python_instance_binary

    # Read the heron_internals.yaml for logging dir
    self.log_dir = self._load_logging_dir(self.heron_internals_config_file)

    # these get set when we call update_packing_plan
    self.packing_plan = None
    self.stmgr_ids = {}
    self.metricsmgr_ids = {}
    self.heron_shell_ids = {}

    # processes_to_monitor gets set once processes are launched. we need to synchronize rw to this
    # dict since is used by multiple threads
    self.process_lock = threading.RLock()
    self.processes_to_monitor = {}

    self.state_managers = []

  @staticmethod
  def parse_args(args):
    """Uses python argparse to collect positional args"""
    parser = argparse.ArgumentParser()

    parser.add_argument("shard", type=int)
    parser.add_argument("topology_name")
    parser.add_argument("topology_id")
    parser.add_argument("topology_defn_file")
    parser.add_argument("zknode")
    parser.add_argument("zkroot")
    parser.add_argument("tmaster_binary")
    parser.add_argument("stmgr_binary")
    parser.add_argument("metricsmgr_classpath")
    parser.add_argument("instance_jvm_opts")
    parser.add_argument("classpath")
    parser.add_argument("master_port")
    parser.add_argument("tmaster_controller_port")
    parser.add_argument("tmaster_stats_port")
    parser.add_argument("heron_internals_config_file")
    parser.add_argument("component_rammap")
    parser.add_argument("component_jvm_opts_in_base64")
    parser.add_argument("pkg_type")
    parser.add_argument("topology_bin_file")
    parser.add_argument("heron_java_home")
    parser.add_argument("shell_port")
    parser.add_argument("heron_shell_binary")
    parser.add_argument("metricsmgr_port")
    parser.add_argument("cluster")
    parser.add_argument("role")
    parser.add_argument("environ")
    parser.add_argument("instance_classpath")
    parser.add_argument("metrics_sinks_config_file")
    parser.add_argument("scheduler_classpath")
    parser.add_argument("scheduler_port")
    parser.add_argument("python_instance_binary")

    parsed_args, unknown_args = parser.parse_known_args(args[1:])

    if unknown_args:
      Log.error('Unknown argument: %s' % unknown_args[0])
      parser.print_help()
      sys.exit(1)

    return parsed_args

  def run_command_or_exit(self, command):
    if self._run_blocking_process(command, True, self.shell_env) != 0:
      Log.error("Failed to run command: %s. Exiting" % command)
      sys.exit(1)

  def initialize(self):
    """
    Initialize the environment. Done with a method call outside of the constructor for 2 reasons:
    1. Unit tests probably won't want/need to do this
    2. We don't initialize the logger (also something unit tests don't want) until after the
    constructor
    """
    create_folders = 'mkdir -p %s' % self.log_dir
    self.run_command_or_exit(create_folders)

    chmod_logs_dir = 'chmod a+rx . && chmod a+x %s' % self.log_dir
    self.run_command_or_exit(chmod_logs_dir)

    chmod_x_binaries = [self.tmaster_binary, self.stmgr_binary, self.heron_shell_binary]

    for binary in chmod_x_binaries:
      stat_result = os.stat(binary)[stat.ST_MODE]
      if not stat_result & stat.S_IXOTH:
        chmod_binary = 'chmod +x %s' % binary
        self.run_command_or_exit(chmod_binary)

    # Log itself pid
    log_pid_for_process(get_heron_executor_process_name(self.shard), os.getpid())

  def update_packing_plan(self, new_packing_plan):
    self.packing_plan = new_packing_plan
    self.stmgr_ids = stmgr_map(self.packing_plan.container_plans)
    self.metricsmgr_ids = metricsmgr_map(self.packing_plan.container_plans)
    self.heron_shell_ids = heron_shell_map(self.packing_plan.container_plans)

  # pylint: disable=no-self-use
  def _load_logging_dir(self, heron_internals_config_file):
    with open(heron_internals_config_file, 'r') as stream:
      heron_internals_config = yaml.load(stream)
    return heron_internals_config['heron.logging.directory']

  def _get_metricsmgr_cmd(self, metricsManagerId, sink_config_file, port):
    ''' get the command to start the metrics manager processes '''
    metricsmgr_main_class = 'com.twitter.heron.metricsmgr.MetricsManager'

    metricsmgr_cmd = [os.path.join(self.heron_java_home, 'bin/java'),
                      # We could not rely on the default -Xmx setting, which could be very big,
                      # for instance, the default -Xmx in Twitter mesos machine is around 18GB
                      '-Xmx1024M',
                      '-XX:+PrintCommandLineFlags',
                      '-verbosegc',
                      '-XX:+PrintGCDetails',
                      '-XX:+PrintGCTimeStamps',
                      '-XX:+PrintGCDateStamps',
                      '-XX:+PrintGCCause',
                      '-XX:+UseGCLogFileRotation',
                      '-XX:NumberOfGCLogFiles=5',
                      '-XX:GCLogFileSize=100M',
                      '-XX:+PrintPromotionFailure',
                      '-XX:+PrintTenuringDistribution',
                      '-XX:+PrintHeapAtGC',
                      '-XX:+HeapDumpOnOutOfMemoryError',
                      '-XX:+UseConcMarkSweepGC',
                      '-XX:+PrintCommandLineFlags',
                      '-Xloggc:log-files/gc.metricsmgr.log',
                      '-Djava.net.preferIPv4Stack=true',
                      '-cp',
                      self.metricsmgr_classpath,
                      metricsmgr_main_class,
                      metricsManagerId,
                      port,
                      self.topology_name,
                      self.topology_id,
                      self.heron_internals_config_file,
                      sink_config_file]

    return metricsmgr_cmd

  def _get_tmaster_processes(self):
    ''' get the command to start the tmaster processes '''
    retval = {}
    tmaster_cmd = [
        self.tmaster_binary,
        self.master_port,
        self.tmaster_controller_port,
        self.tmaster_stats_port,
        self.topology_name,
        self.topology_id,
        self.zknode,
        self.zkroot,
        ','.join(self.stmgr_ids.values()),
        self.heron_internals_config_file,
        self.metrics_sinks_config_file,
        self.metricsmgr_port]
    retval["heron-tmaster"] = tmaster_cmd

    # metricsmgr_metrics_sink_config_file = 'metrics_sinks.yaml'

    retval[self.metricsmgr_ids[0]] = self._get_metricsmgr_cmd(
        self.metricsmgr_ids[0],
        self.metrics_sinks_config_file,
        self.metricsmgr_port)

    return retval

  # Returns the processes for each Java Heron Instance
  def _get_java_instance_cmd(self, instance_info):
    retval = {}
    # TO DO (Karthik) to be moved into keys and defaults files
    code_cache_size_mb = 64
    perm_gen_size_mb = 128
    for (instance_id, component_name, global_task_id, component_index) in instance_info:
      total_jvm_size = int(self.component_rammap[component_name] / (1024 * 1024))
      heap_size_mb = total_jvm_size - code_cache_size_mb - perm_gen_size_mb
      Log.info("component name: %s, ram request: %d, total jvm size: %dM, "
               "cache size: %dM, perm size: %dM"
               % (component_name, self.component_rammap[component_name],
                  total_jvm_size, code_cache_size_mb, perm_gen_size_mb))
      xmn_size = int(heap_size_mb / 2)
      instance_cmd = [os.path.join(self.heron_java_home, 'bin/java'),
                      '-Xmx%dM' % heap_size_mb,
                      '-Xms%dM' % heap_size_mb,
                      '-Xmn%dM' % xmn_size,
                      '-XX:MaxPermSize=%dM' % perm_gen_size_mb,
                      '-XX:PermSize=%dM' % perm_gen_size_mb,
                      '-XX:ReservedCodeCacheSize=%dM' % code_cache_size_mb,
                      '-XX:+CMSScavengeBeforeRemark',
                      '-XX:TargetSurvivorRatio=90',
                      '-XX:+PrintCommandLineFlags',
                      '-verbosegc',
                      '-XX:+PrintGCDetails',
                      '-XX:+PrintGCTimeStamps',
                      '-XX:+PrintGCDateStamps',
                      '-XX:+PrintGCCause',
                      '-XX:+UseGCLogFileRotation',
                      '-XX:NumberOfGCLogFiles=5',
                      '-XX:GCLogFileSize=100M',
                      '-XX:+PrintPromotionFailure',
                      '-XX:+PrintTenuringDistribution',
                      '-XX:+PrintHeapAtGC',
                      '-XX:+HeapDumpOnOutOfMemoryError',
                      '-XX:+UseConcMarkSweepGC',
                      '-XX:ParallelGCThreads=4',
                      '-Xloggc:log-files/gc.%s.log' % instance_id]
      instance_cmd = instance_cmd + self.instance_jvm_opts.split()
      if component_name in self.component_jvm_opts:
        instance_cmd = instance_cmd + self.component_jvm_opts[component_name].split()
      instance_cmd.extend(['-Djava.net.preferIPv4Stack=true',
                           '-cp',
                           '%s:%s' % (self.instance_classpath, self.classpath),
                           'com.twitter.heron.instance.HeronInstance',
                           self.topology_name,
                           self.topology_id,
                           instance_id,
                           component_name,
                           str(global_task_id),
                           str(component_index),
                           self.stmgr_ids[self.shard],
                           self.master_port,
                           self.metricsmgr_port,
                           self.heron_internals_config_file])
      retval[instance_id] = instance_cmd
    return retval

  # Returns the processes for each Python Heron Instance
  def _get_python_instance_cmd(self, instance_info):
    # pylint: disable=fixme
    # TODO: currently ignoring ramsize, heap, etc.
    retval = {}
    for (instance_id, component_name, global_task_id, component_index) in instance_info:
      Log.info("Python instance %s component: %s" %(instance_id, component_name))
      instance_cmd = [self.python_instance_binary,
                      self.topology_name,
                      self.topology_id,
                      instance_id,
                      component_name,
                      str(global_task_id),
                      str(component_index),
                      self.stmgr_ids[self.shard],
                      self.master_port,
                      self.metricsmgr_port,
                      self.heron_internals_config_file,
                      self.topology_bin_file]

      retval[instance_id] = instance_cmd

    return retval

  # Returns the processes to handle streams, including the stream-mgr and the user code containing
  # the stream logic of the topology
  def _get_streaming_processes(self):
    '''
    Returns the processes to handle streams, including the stream-mgr and the user code containing
    the stream logic of the topology
    '''
    retval = {}
    instance_plans = self._get_instance_plans(self.packing_plan, self.shard)
    instance_info = []
    for instance_plan in instance_plans:
      global_task_id = instance_plan.task_id
      component_index = instance_plan.component_index
      component_name = instance_plan.component_name
      instance_id = "container_%s_%s_%d" % (str(self.shard), component_name, global_task_id)
      instance_info.append((instance_id, component_name, global_task_id, component_index))

    stmgr_cmd = [
        self.stmgr_binary,
        self.topology_name,
        self.topology_id,
        self.topology_defn_file,
        self.zknode,
        self.zkroot,
        self.stmgr_ids[self.shard],
        ','.join(map(lambda x: x[0], instance_info)),
        self.master_port,
        self.metricsmgr_port,
        self.shell_port,
        self.heron_internals_config_file]
    retval[self.stmgr_ids[self.shard]] = stmgr_cmd

    # metricsmgr_metrics_sink_config_file = 'metrics_sinks.yaml'

    retval[self.metricsmgr_ids[self.shard]] = self._get_metricsmgr_cmd(
        self.metricsmgr_ids[self.shard],
        self.metrics_sinks_config_file,
        self.metricsmgr_port
    )

    if self.pkg_type == 'jar' or self.pkg_type == 'tar':
      retval.update(self._get_java_instance_cmd(instance_info))
    elif self.pkg_type == 'pex':
      retval.update(self._get_python_instance_cmd(instance_info))
    else:
      raise ValueError("Unrecognized package type: %s" % self.pkg_type)

    return retval

  def _get_instance_plans(self, packing_plan, container_id):
    """
    For the given packing_plan, return the container plan with the given container_id. If protobufs
    supported maps, we could just get the plan by id, but it doesn't so we have a collection of
    containers to iterate over.
    """
    this_container_plan = None
    for container_plan in packing_plan.container_plans:
      if container_plan.id == container_id:
        this_container_plan = container_plan

    # make sure that our shard id is a valid one
    assert this_container_plan is not None
    return this_container_plan.instance_plans

  # Returns the common heron support processes that all containers get, like the heron shell
  def _get_heron_support_processes(self):
    """ Get a map from all daemon services' name to the command to start them """
    retval = {}

    retval[self.heron_shell_ids[self.shard]] = [
        '%s' % self.heron_shell_binary,
        '--port=%s' % self.shell_port,
        '--log_file_prefix=%s/heron-shell.log' % self.log_dir]

    return retval

  def _untar_if_tar(self):
    if self.pkg_type == "tar":
      os.system("tar -xvf %s" % self.topology_bin_file)

  # pylint: disable=no-self-use
  def _wait_process_std_out_err(self, name, process):
    ''' Wait for the termination of a process and log its stdout & stderr '''
    (process_stdout, process_stderr) = process.communicate()
    if process_stdout:
      Log.info("%s stdout: %s" %(name, process_stdout))
    if process_stderr:
      Log.info("%s stderr: %s" %(name, process_stderr))

  def _run_process(self, name, cmd, env_to_exec=None):
    Log.info("Running %s process as %s" % (name, ' '.join(cmd)))
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            env=env_to_exec)

  def _run_blocking_process(self, cmd, is_shell, env_to_exec=None):
    Log.info("Running blocking process as %s" % cmd)
    process = subprocess.Popen(cmd, shell=is_shell, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=env_to_exec)

    # wait for termination
    self._wait_process_std_out_err("", process)

    # return the exit code
    return process.returncode

  def _kill_processes(self, commands):
    # remove the command from processes_to_monitor and kill the process
    with self.process_lock:
      for command_name, command in commands.iteritems():
        for process_info in self.processes_to_monitor.values():
          if process_info.name == command_name:
            del self.processes_to_monitor[process_info.pid]
            Log.info("Killing %s process with pid %d: %s" %
                     (process_info.name, process_info.pid, ' '.join(command)))
            try:
              process_info.process.kill()
            except OSError, e:
              if e.errno == 3: # No such process
                Log.warn("Expected process %s with pid %d was not running, ignoring." %
                         (process_info.name, process_info.pid))
              else:
                raise e

  def _start_processes(self, commands):
    """Start all commands and add them to the dict of processes to be monitored """
    processes_to_monitor = {}
    # First start all the processes
    for (name, command) in commands.items():
      p = self._run_process(name, command, self.shell_env)
      processes_to_monitor[p.pid] = ProcessInfo(p, name, command)

      # Log down the pid file
      log_pid_for_process(name, p.pid)

    with self.process_lock:
      self.processes_to_monitor.update(processes_to_monitor)

  def start_process_monitor(self):
    """ Monitor all processes in processes_to_monitor dict,
    restarting any if they fail, up to max_runs times.
    """
    # Now wait for any child to die
    while True:
      if len(self.processes_to_monitor) > 0:
        (pid, status) = os.wait()

        with self.process_lock:
          if pid in self.processes_to_monitor.keys():
            old_process_info = self.processes_to_monitor[pid]
            name = old_process_info.name
            command = old_process_info.command
            Log.info("%s (pid=%s) exited with status %d. command=%s" % (name, pid, status, command))
            # Log the stdout & stderr of the failed process
            self._wait_process_std_out_err(name, old_process_info.process)

            # Just make it world readable
            if os.path.isfile("core.%d" % pid):
              os.system("chmod a+r core.%d" % pid)
            if old_process_info.attempts >= self.max_runs:
              Log.info("%s exited too many times" % name)
              sys.exit(1)
            time.sleep(self.interval_between_runs)
            p = self._run_process(name, command)
            del self.processes_to_monitor[pid]
            self.processes_to_monitor[p.pid] =\
              ProcessInfo(p, name, command, old_process_info.attempts + 1)

            # Log down the pid file
            log_pid_for_process(name, p.pid)

  def get_commands_to_run(self):
    # During shutdown the watch might get triggered with the empty packing plan
    if len(self.packing_plan.container_plans) == 0:
      return {}

    if self.shard == 0:
      commands = self._get_tmaster_processes()
    else:
      self._untar_if_tar()
      commands = self._get_streaming_processes()

    # Attach daemon processes
    commands.update(self._get_heron_support_processes())
    return commands

  def get_command_changes(self, current_commands, updated_commands):
    """
    Compares the current command with updated command to return a 3-tuple of dicts,
    keyed by command name: commands_to_kill, commands_to_keep and commands_to_start.
    """
    commands_to_kill = {}
    commands_to_keep = {}
    commands_to_start = {}

    # if the current command has a matching command in the updated commands we keep it
    # otherwise we kill it
    for current_name, current_command in current_commands.iteritems():
      # Always restart tmaster to pick up new state. The stream manager is also restarted, but
      # we shouldn't need to do that and work is being done to fix that on the steam manager
      if current_name in updated_commands.keys() and \
        current_command == updated_commands[current_name] and \
        current_name != 'heron-tmaster' and \
        not current_name.startswith('stmgr-'):
        commands_to_keep[current_name] = current_command
      else:
        commands_to_kill[current_name] = current_command

    # updated commands not in the keep list need to be started
    for updated_name, updated_command in updated_commands.iteritems():
      if updated_name not in commands_to_keep.keys():
        commands_to_start[updated_name] = updated_command

    return commands_to_kill, commands_to_keep, commands_to_start

  def launch(self):
    ''' Determines the commands to be run and compares them with the existing running commands.
    Then starts new ones required and kills old ones no longer required.
    '''
    with self.process_lock:
      current_commands = dict(map((lambda process: (process.name, process.command)),
                                  self.processes_to_monitor.values()))
      updated_commands = self.get_commands_to_run()

      # get the commands to kill, keep and start
      commands_to_kill, commands_to_keep, commands_to_start = \
          self.get_command_changes(current_commands, updated_commands)

      Log.info("current commands: %s" % sorted(current_commands.keys()))
      Log.info("new commands    : %s" % sorted(updated_commands.keys()))
      Log.info("commands_to_kill: %s" % sorted(commands_to_kill.keys()))
      Log.info("commands_to_keep: %s" % sorted(commands_to_keep.keys()))
      Log.info("commands_to_start: %s" % sorted(commands_to_start.keys()))

      self._kill_processes(commands_to_kill)
      self._start_processes(commands_to_start)
      Log.info("Launch complete - processes killed=%s kept=%s started=%s monitored=%s" %
               (len(commands_to_kill), len(commands_to_keep),
                len(commands_to_start), len(self.processes_to_monitor)))

  # pylint: disable=global-statement
  def start_state_manager_watches(self):
    """
    Receive updates to the packing plan from the statemgrs and update processes as needed.
    """
    statemgr_config = StateMgrConfig()
    statemgr_config.set_state_locations(configloader.load_state_manager_locations(self.cluster))
    self.state_managers = statemanagerfactory.get_all_state_managers(statemgr_config)

    # pylint: disable=unused-argument
    def on_packing_plan_watch(state_manager, new_packing_plan):
      Log.debug("State watch triggered for PackingPlan update on shard %s. Existing: %s, New: %s" %
                (self.shard, str(self.packing_plan), str(new_packing_plan)))

      if self.packing_plan != new_packing_plan:
        Log.info("PackingPlan change detected on shard %s, relaunching effected processes."
                 % self.shard)
        self.update_packing_plan(new_packing_plan)

        Log.info("Updating executor processes")
        self.launch()
      else:
        Log.info(
            "State watch triggered for PackingPlan update but plan not changed so not relaunching.")

    for state_manager in self.state_managers:
      # The callback function with the bound
      # state_manager as first variable.
      onPackingPlanWatch = partial(on_packing_plan_watch, state_manager)
      state_manager.get_packing_plan(self.topology_name, onPackingPlanWatch)
      Log.info("Registered state watch for packing plan changes with state manager %s." %
               str(state_manager))

  def stop_state_manager_watches(self):
    Log.info("Stopping state managers")
    for state_manager in self.state_managers:
      state_manager.stop()

def main():
  """Register exit handlers, initialize the executor and run it."""
  # Since Heron on YARN runs as headless users, pex compiled
  # binaries should be exploded into the container working
  # directory. In order to do this, we need to set the
  # PEX_ROOT shell environment before forking the processes
  shell_env = os.environ.copy()
  shell_env["PEX_ROOT"] = os.path.join(os.path.abspath('.'), ".pex")

  # Instantiate the executor, bind it to signal handlers and launch it
  executor = HeronExecutor(sys.argv, shell_env)

  # pylint: disable=unused-argument
  def signal_handler(signal_to_handle, frame):
    # We would do nothing here but just exit
    # Just catch the SIGTERM and then cleanup(), registered with atexit, would invoke
    Log.info('signal_handler invoked with signal %s', signal_to_handle)
    executor.stop_state_manager_watches()
    sys.exit(signal_to_handle)

  def setup(shardid):
    # Redirect stdout and stderr to files in append mode
    # The filename format is heron-executor-<container_id>.stdxxx
    log.configure(logfile='heron-executor-%s.stdout' % shardid, with_time=True)

    Log.info('Set up process group; executor becomes leader')
    os.setpgrp() # create new process group, become its leader

    Log.info('Register the SIGTERM signal handler')
    signal.signal(signal.SIGTERM, signal_handler)

    Log.info('Register the atexit clean up')
    atexit.register(cleanup)

  def cleanup():
    """Handler to trigger when receiving the SIGTERM signal
    Do cleanup inside this method, including:
    1. Terminate all children processes
    """
    Log.info('Executor terminated; exiting all process in executor.')
    # We would not wait or check whether process spawned dead or not
    os.killpg(0, signal.SIGTERM)

  setup(executor.shard)

  executor.initialize()
  executor.start_state_manager_watches()
  executor.start_process_monitor()

if __name__ == "__main__":
  main()
