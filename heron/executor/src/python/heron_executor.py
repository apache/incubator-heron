#!/usr/bin/env python2.7
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

""" The Heron executor is a process that runs on a container and is responsible for starting and
monitoring the processes of the topology and it's support services."""
import argparse
import atexit
import base64
import functools
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
import socket
import traceback
import itertools

from heron.common.src.python.utils import log
from heron.common.src.python.utils import proc
# pylint: disable=unused-import,too-many-lines
from heron.proto.packing_plan_pb2 import PackingPlan
from heron.statemgrs.src.python import statemanagerfactory
from heron.statemgrs.src.python import configloader
from heron.statemgrs.src.python.config import Config as StateMgrConfig

Log = log.Log

# pylint: disable=too-many-lines

def print_usage():
  print(
      "Usage: ./heron-executor --shard=<shardid> --topology-name=<topname>"
      " --topology-id=<topid> --topology-defn-file=<topdefnfile>"
      " --state-manager-connection=<state_manager_connection>"
      " --state-manager-root=<state_manager_root>"
      " --state-manager-config-file=<state_manager_config_file>"
      " --tmaster-binary=<tmaster_binary>"
      " --stmgr-binary=<stmgr_binary> --metrics-manager-classpath=<metricsmgr_classpath>"
      " --instance-jvm-opts=<instance_jvm_opts_in_base64> --classpath=<classpath>"
      " --master-port=<master_port> --tmaster-controller-port=<tmaster_controller_port>"
      " --tmaster-stats-port=<tmaster_stats_port>"
      " --heron-internals-config-file=<heron_internals_config_file>"
      " --override-config-file=<override_config_file> --component-ram-map=<component_ram_map>"
      " --component-jvm-opts=<component_jvm_opts_in_base64> --pkg-type=<pkg_type>"
      " --topology-binary-file=<topology_bin_file> --heron-java-home=<heron_java_home>"
      " --shell-port=<shell-port> --heron-shell-binary=<heron_shell_binary>"
      " --metrics-manager-port=<metricsmgr_port>"
      " --cluster=<cluster> --role=<role> --environment=<environ>"
      " --instance-classpath=<instance_classpath>"
      " --metrics-sinks-config-file=<metrics_sinks_config_file>"
      " --scheduler-classpath=<scheduler_classpath> --scheduler-port=<scheduler_port>"
      " --python-instance-binary=<python_instance_binary>"
      " --metricscache-manager-classpath=<metricscachemgr_classpath>"
      " --metricscache-manager-master-port=<metricscachemgr_masterport>"
      " --metricscache-manager-stats-port=<metricscachemgr_statsport>"
      " --is-stateful=<is_stateful> --checkpoint-manager-classpath=<ckptmgr_classpath>"
      " --checkpoint-manager-port=<ckptmgr_port> --checkpoint-manager-ram=<checkpoint_manager_ram>"
      " --stateful-config-file=<stateful_config_file>"
      " --health-manager-mode=<healthmgr_mode> --health-manager-classpath=<healthmgr_classpath>"
      " --cpp-instance-binary=<cpp_instance_binary>"
      " --jvm-remote-debugger-ports=<comma_seperated_port_list>")

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

def ckptmgr_map(container_plans):
  return id_map("ckptmgr", container_plans, True)

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

def is_docker_environment():
  return os.path.isfile('/.dockerenv')

def stdout_log_fn(cmd):
  """Simple function callback that is used to log the streaming output of a subprocess command
  :param cmd: the name of the command which will be added to the log line
  :return: None
  """
  # Log the messages to stdout and strip off the newline because Log.info adds one automatically
  return lambda line: Log.info("%s stdout: %s", cmd, line.rstrip('\n'))

class Command(object):
  """
  Command to run as a separate process using subprocess.POpen
  :param cmd: command to run (as a list)
  :param env: env variables for the process (as a map)
  """
  def __init__(self, cmd, env):
    if isinstance(cmd, list):
      self.cmd = cmd
    else:
      self.cmd = [cmd]
    self.env = env

  def extend(self, args):
    self.cmd.extend(args)

  def append(self, arg):
    self.cmd.append(arg)

  def copy(self):
    return Command(list(self.cmd), self.env.copy() if self.env is not None else {})

  def __repr__(self):
    return str(self.cmd)

  def __str__(self):
    return ' '.join(self.cmd)

  def __eq__(self, other):
    return self.cmd == other.cmd

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
    self.command_str = command.__str__() # convenience for unit tests
    self.attempts = attempts

  def increment_attempts(self):
    self.attempts += 1
    return self

# pylint: disable=too-many-instance-attributes,too-many-statements
class HeronExecutor(object):
  """ Heron executor is a class that is responsible for running each of the process on a given
  container. Based on the container id and the instance distribution, it determines if the container
  is a master node or a worker node and it starts processes accordingly."""
  def init_from_parsed_args(self, parsed_args):
    """ initialize from parsed arguments """
    self.shard = parsed_args.shard
    self.topology_name = parsed_args.topology_name
    self.topology_id = parsed_args.topology_id
    self.topology_defn_file = parsed_args.topology_defn_file
    self.state_manager_connection = parsed_args.state_manager_connection
    self.state_manager_root = parsed_args.state_manager_root
    self.state_manager_config_file = parsed_args.state_manager_config_file
    self.tmaster_binary = parsed_args.tmaster_binary
    self.stmgr_binary = parsed_args.stmgr_binary
    self.metrics_manager_classpath = parsed_args.metrics_manager_classpath
    self.metricscache_manager_classpath = parsed_args.metricscache_manager_classpath
    # '=' can be parsed in a wrong way by some schedulers (aurora) hence it needs to be escaped.
    # It is escaped in two different ways. '(61)' is the new escaping. '&equals;' was
    # the original replacement but it is not friendly to bash and is causing issues. The original
    # escaping is still left there for reference and backward compatibility purposes (to be
    # removed after no topology needs it)
    self.instance_jvm_opts =\
        base64.b64decode(parsed_args.instance_jvm_opts.lstrip('"').
                         rstrip('"').replace('(61)', '=').replace('&equals;', '='))
    self.classpath = parsed_args.classpath
    # Needed for Docker environments since the hostname of a docker container is the container's
    # id within docker, rather than the host's hostname. NOTE: this 'HOST' env variable is not
    # guaranteed to be set in all Docker executor environments (outside of Marathon)
    if is_docker_environment():
      self.master_host = os.environ.get('HOST') if 'HOST' in os.environ else socket.gethostname()
    else:
      self.master_host = socket.gethostname()
    self.master_port = parsed_args.master_port
    self.tmaster_controller_port = parsed_args.tmaster_controller_port
    self.tmaster_stats_port = parsed_args.tmaster_stats_port
    self.heron_internals_config_file = parsed_args.heron_internals_config_file
    self.override_config_file = parsed_args.override_config_file
    self.component_ram_map = [{x.split(':')[0]:int(x.split(':')[1])}
                              for x in parsed_args.component_ram_map.split(',')]
    self.component_ram_map = functools.reduce(lambda x, y: dict(list(x.items()) + list(y.items())),
                                              self.component_ram_map)

    # component_jvm_opts_in_base64 itself is a base64-encoding-json-map, which is appended with
    # " at the start and end. It also escapes "=" to "&equals" due to aurora limitation
    # And the json is a map from base64-encoding-component-name to base64-encoding-jvm-options
    self.component_jvm_opts = {}
    # First we need to decode the base64 string back to a json map string.
    # '=' can be parsed in a wrong way by some schedulers (aurora) hence it needs to be escaped.
    # It is escaped in two different ways. '(61)' is the new escaping. '&equals;' was
    # the original replacement but it is not friendly to bash and is causing issues. The original
    # escaping is still left there for reference and backward compatibility purposes (to be
    # removed after no topology needs it)
    component_jvm_opts_in_json =\
        base64.b64decode(parsed_args.component_jvm_opts.
                         lstrip('"').rstrip('"').replace('(61)', '=').replace('&equals;', '='))
    if component_jvm_opts_in_json != "":
      for (k, v) in list(json.loads(component_jvm_opts_in_json).items()):
        # In json, the component name and JVM options are still in base64 encoding
        self.component_jvm_opts[base64.b64decode(k)] = base64.b64decode(v)

    self.pkg_type = parsed_args.pkg_type
    self.topology_binary_file = parsed_args.topology_binary_file
    self.heron_java_home = parsed_args.heron_java_home
    self.shell_port = parsed_args.shell_port
    self.heron_shell_binary = parsed_args.heron_shell_binary
    self.metrics_manager_port = parsed_args.metrics_manager_port
    self.metricscache_manager_master_port = parsed_args.metricscache_manager_master_port
    self.metricscache_manager_stats_port = parsed_args.metricscache_manager_stats_port
    self.cluster = parsed_args.cluster
    self.role = parsed_args.role
    self.environment = parsed_args.environment
    self.instance_classpath = parsed_args.instance_classpath
    self.metrics_sinks_config_file = parsed_args.metrics_sinks_config_file
    self.scheduler_classpath = parsed_args.scheduler_classpath
    self.scheduler_port = parsed_args.scheduler_port
    self.python_instance_binary = parsed_args.python_instance_binary
    self.cpp_instance_binary = parsed_args.cpp_instance_binary

    self.is_stateful_topology = (parsed_args.is_stateful.lower() == 'true')
    self.checkpoint_manager_classpath = parsed_args.checkpoint_manager_classpath
    self.checkpoint_manager_port = parsed_args.checkpoint_manager_port
    self.checkpoint_manager_ram = parsed_args.checkpoint_manager_ram
    self.stateful_config_file = parsed_args.stateful_config_file
    self.metricscache_manager_mode = parsed_args.metricscache_manager_mode \
        if parsed_args.metricscache_manager_mode else "disabled"
    self.health_manager_mode = parsed_args.health_manager_mode
    self.health_manager_classpath = '%s:%s'\
        % (self.scheduler_classpath, parsed_args.health_manager_classpath)
    self.jvm_remote_debugger_ports = \
      parsed_args.jvm_remote_debugger_ports.split(",") \
        if parsed_args.jvm_remote_debugger_ports else None

  def __init__(self, args, shell_env):
    parsed_args = self.parse_args(args)
    self.init_from_parsed_args(parsed_args)

    self.shell_env = shell_env
    self.max_runs = 150
    self.interval_between_runs = 10

    # Read the heron_internals.yaml for logging dir
    self.log_dir = self._load_logging_dir(self.heron_internals_config_file)

    # these get set when we call update_packing_plan
    self.packing_plan = None
    self.stmgr_ids = {}
    self.metricsmgr_ids = {}
    self.heron_shell_ids = {}
    self.ckptmgr_ids = {}

    # processes_to_monitor gets set once processes are launched. we need to synchronize rw to this
    # dict since is used by multiple threads
    self.process_lock = threading.RLock()
    self.processes_to_monitor = {}

    self.state_managers = []
    self.jvm_version = None

  @staticmethod
  def parse_args(args):
    """Uses python argparse to collect positional args"""
    Log.info("Input args: %r" % args)

    parser = argparse.ArgumentParser()

    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--topology-name", required=True)
    parser.add_argument("--topology-id", required=True)
    parser.add_argument("--topology-defn-file", required=True)
    parser.add_argument("--state-manager-connection", required=True)
    parser.add_argument("--state-manager-root", required=True)
    parser.add_argument("--state-manager-config-file", required=True)
    parser.add_argument("--tmaster-binary", required=True)
    parser.add_argument("--stmgr-binary", required=True)
    parser.add_argument("--metrics-manager-classpath", required=True)
    parser.add_argument("--instance-jvm-opts", required=True)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--master-port", required=True)
    parser.add_argument("--tmaster-controller-port", required=True)
    parser.add_argument("--tmaster-stats-port", required=True)
    parser.add_argument("--heron-internals-config-file", required=True)
    parser.add_argument("--override-config-file", required=True)
    parser.add_argument("--component-ram-map", required=True)
    parser.add_argument("--component-jvm-opts", required=True)
    parser.add_argument("--pkg-type", required=True)
    parser.add_argument("--topology-binary-file", required=True)
    parser.add_argument("--heron-java-home", required=True)
    parser.add_argument("--shell-port", required=True)
    parser.add_argument("--heron-shell-binary", required=True)
    parser.add_argument("--metrics-manager-port", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--instance-classpath", required=True)
    parser.add_argument("--metrics-sinks-config-file", required=True)
    parser.add_argument("--scheduler-classpath", required=True)
    parser.add_argument("--scheduler-port", required=True)
    parser.add_argument("--python-instance-binary", required=True)
    parser.add_argument("--cpp-instance-binary", required=True)
    parser.add_argument("--metricscache-manager-classpath", required=True)
    parser.add_argument("--metricscache-manager-master-port", required=True)
    parser.add_argument("--metricscache-manager-stats-port", required=True)
    parser.add_argument("--metricscache-manager-mode", required=False)
    parser.add_argument("--is-stateful", required=True)
    parser.add_argument("--checkpoint-manager-classpath", required=True)
    parser.add_argument("--checkpoint-manager-port", required=True)
    parser.add_argument("--checkpoint-manager-ram", type=int, required=True)
    parser.add_argument("--stateful-config-file", required=True)
    parser.add_argument("--health-manager-mode", required=True)
    parser.add_argument("--health-manager-classpath", required=True)
    parser.add_argument("--jvm-remote-debugger-ports", required=False,
                        help="ports to be used by a remote debugger for JVM instances")

    parsed_args, unknown_args = parser.parse_known_args(args[1:])

    if unknown_args:
      Log.warn('Unknown arguments found!!! They are: %s' % unknown_args)
      Log.warn(parser.format_help())

    return parsed_args

  def run_command_or_exit(self, command):
    if self._run_blocking_process(command, True) != 0:
      Log.error("Failed to run command: %s. Exiting" % command)
      sys.exit(1)

  def initialize(self):
    """
    Initialize the environment. Done with a method call outside of the constructor for 2 reasons:
    1. Unit tests probably won't want/need to do this
    2. We don't initialize the logger (also something unit tests don't want) until after the
    constructor
    """
    create_folders = Command('mkdir -p %s' % self.log_dir, self.shell_env)
    self.run_command_or_exit(create_folders)

    chmod_logs_dir = Command('chmod a+rx . && chmod a+x %s' % self.log_dir, self.shell_env)
    self.run_command_or_exit(chmod_logs_dir)

    chmod_x_binaries = [self.tmaster_binary, self.stmgr_binary, self.heron_shell_binary]

    for binary in chmod_x_binaries:
      stat_result = os.stat(binary)[stat.ST_MODE]
      if not stat_result & stat.S_IXOTH:
        chmod_binary = Command('chmod +x %s' % binary, self.shell_env)
        self.run_command_or_exit(chmod_binary)

    # Log itself pid
    log_pid_for_process(get_heron_executor_process_name(self.shard), os.getpid())

  def update_packing_plan(self, new_packing_plan):
    self.packing_plan = new_packing_plan
    self.stmgr_ids = stmgr_map(self.packing_plan.container_plans)
    self.ckptmgr_ids = ckptmgr_map(self.packing_plan.container_plans)
    self.metricsmgr_ids = metricsmgr_map(self.packing_plan.container_plans)
    self.heron_shell_ids = heron_shell_map(self.packing_plan.container_plans)

  # pylint: disable=no-self-use
  def _load_logging_dir(self, heron_internals_config_file):
    with open(heron_internals_config_file, 'r') as stream:
      heron_internals_config = yaml.load(stream)
    return heron_internals_config['heron.logging.directory']

  def _get_metricsmgr_cmd(self, metricsManagerId, sink_config_file, port):
    ''' get the command to start the metrics manager processes '''
    metricsmgr_main_class = 'org.apache.heron.metricsmgr.MetricsManager'

    metricsmgr_cmd = [os.path.join(self.heron_java_home, 'bin/java'),
                      # We could not rely on the default -Xmx setting, which could be very big,
                      # for instance, the default -Xmx in Twitter mesos machine is around 18GB
                      '-Xmx1024M',
                      '-XX:+PrintCommandLineFlags',
                      '-Djava.net.preferIPv4Stack=true',
                      '-cp',
                      self.metrics_manager_classpath,
                      metricsmgr_main_class,
                      '--id=' + metricsManagerId,
                      '--port=' + str(port),
                      '--topology=' + self.topology_name,
                      '--cluster=' + self.cluster,
                      '--role=' + self.role,
                      '--environment=' + self.environment,
                      '--topology-id=' + self.topology_id,
                      '--system-config-file=' + self.heron_internals_config_file,
                      '--override-config-file=' + self.override_config_file,
                      '--sink-config-file=' + sink_config_file]

    # Insert GC Options
    metricsmgr_cmd = self._get_java_gc_instance_cmd(metricsmgr_cmd, metricsManagerId)
    return Command(metricsmgr_cmd, self.shell_env)

  def _get_metrics_cache_cmd(self):
    ''' get the command to start the metrics manager processes '''
    metricscachemgr_main_class = 'org.apache.heron.metricscachemgr.MetricsCacheManager'

    metricscachemgr_cmd = [os.path.join(self.heron_java_home, 'bin/java'),
                           # We could not rely on the default -Xmx setting, which could be very big,
                           # for instance, the default -Xmx in Twitter mesos machine is around 18GB
                           '-Xmx1024M',
                           '-XX:+PrintCommandLineFlags',
                           '-Djava.net.preferIPv4Stack=true',
                           '-cp',
                           self.metricscache_manager_classpath,
                           metricscachemgr_main_class,
                           "--metricscache_id", 'metricscache-0',
                           "--master_port", self.metricscache_manager_master_port,
                           "--stats_port", self.metricscache_manager_stats_port,
                           "--topology_name", self.topology_name,
                           "--topology_id", self.topology_id,
                           "--system_config_file", self.heron_internals_config_file,
                           "--override_config_file", self.override_config_file,
                           "--sink_config_file", self.metrics_sinks_config_file,
                           "--cluster", self.cluster,
                           "--role", self.role,
                           "--environment", self.environment]

    # Insert GC Options
    metricscachemgr_cmd = self._get_java_gc_instance_cmd(metricscachemgr_cmd, 'metricscache')
    return Command(metricscachemgr_cmd, self.shell_env)

  def _get_healthmgr_cmd(self):
    ''' get the command to start the topology health manager processes '''
    healthmgr_main_class = 'org.apache.heron.healthmgr.HealthManager'

    healthmgr_cmd = [os.path.join(self.heron_java_home, 'bin/java'),
                     # We could not rely on the default -Xmx setting, which could be very big,
                     # for instance, the default -Xmx in Twitter mesos machine is around 18GB
                     '-Xmx1024M',
                     '-XX:+PrintCommandLineFlags',
                     '-Djava.net.preferIPv4Stack=true',
                     '-cp', self.health_manager_classpath,
                     healthmgr_main_class,
                     "--cluster", self.cluster,
                     "--role", self.role,
                     "--environment", self.environment,
                     "--topology_name", self.topology_name,
                     "--metricsmgr_port", self.metrics_manager_port]

    # Insert GC Options
    healthmgr_cmd = self._get_java_gc_instance_cmd(healthmgr_cmd, 'healthmgr')
    return Command(healthmgr_cmd, self.shell_env)

  def _get_tmaster_processes(self):
    ''' get the command to start the tmaster processes '''
    retval = {}
    tmaster_cmd_lst = [
        self.tmaster_binary,
        '--topology_name=%s' % self.topology_name,
        '--topology_id=%s' % self.topology_id,
        '--zkhostportlist=%s' % self.state_manager_connection,
        '--zkroot=%s' % self.state_manager_root,
        '--myhost=%s' % self.master_host,
        '--master_port=%s' % str(self.master_port),
        '--controller_port=%s' % str(self.tmaster_controller_port),
        '--stats_port=%s' % str(self.tmaster_stats_port),
        '--config_file=%s' % self.heron_internals_config_file,
        '--override_config_file=%s' % self.override_config_file,
        '--metrics_sinks_yaml=%s' % self.metrics_sinks_config_file,
        '--metricsmgr_port=%s' % str(self.metrics_manager_port),
        '--ckptmgr_port=%s' % str(self.checkpoint_manager_port)]

    tmaster_env = self.shell_env.copy() if self.shell_env is not None else {}
    tmaster_cmd = Command(tmaster_cmd_lst, tmaster_env)
    if os.environ.get('ENABLE_HEAPCHECK') is not None:
      tmaster_cmd.env.update({
          'LD_PRELOAD': "/usr/lib/libtcmalloc.so",
          'HEAPCHECK': "normal"
      })

    retval["heron-tmaster"] = tmaster_cmd

    if self.metricscache_manager_mode.lower() != "disabled":
      retval["heron-metricscache"] = self._get_metrics_cache_cmd()

    if self.health_manager_mode.lower() != "disabled":
      retval["heron-healthmgr"] = self._get_healthmgr_cmd()

    retval[self.metricsmgr_ids[0]] = self._get_metricsmgr_cmd(
        self.metricsmgr_ids[0],
        self.metrics_sinks_config_file,
        self.metrics_manager_port)

    if self.is_stateful_topology:
      retval.update(self._get_ckptmgr_process())

    return retval

  # Returns the processes for each Java Heron Instance
  def _get_java_instance_cmd(self, instance_info):
    retval = {}
    # TO DO (Karthik) to be moved into keys and defaults files
    instance_class_name = 'org.apache.heron.instance.HeronInstance'

    if self.jvm_remote_debugger_ports and \
            (len(instance_info) > len(self.jvm_remote_debugger_ports)):
      Log.warn("Not enough remote debugger ports for all instances!")

    # Create id to java command map
    for (instance_id, component_name, global_task_id, component_index) in instance_info:
      # Append debugger ports
      remote_debugger_port = None
      if self.jvm_remote_debugger_ports:
        remote_debugger_port = self.jvm_remote_debugger_ports.pop()

      instance_cmd = self._get_jvm_instance_cmd().copy()            # JVM command
      instance_cmd.extend(                                          # JVM options
          self._get_jvm_instance_options(
              instance_id, component_name, remote_debugger_port))
      instance_cmd.append(instance_class_name)                      # Class name
      instance_cmd.extend(                                          # JVM arguments
          self._get_jvm_instance_arguments(
              instance_id, component_name, global_task_id, component_index, remote_debugger_port))

      retval[instance_id] = instance_cmd

    return retval

  def _get_jvm_instance_cmd(self):
    return Command(os.path.join(self.heron_java_home, 'bin/java'), self.shell_env)

  def _get_java_major_version(self):
    return int(self._get_jvm_version().split(".")[0])

  def _get_java_gc_instance_cmd(self, cmd, gc_name):
    gc_cmd = ['-verbosegc']
    if self._get_java_major_version() >= 9:
      gc_cmd += [
          '-XX:+UseG1GC',
          '-XX:+ParallelRefProcEnabled',
          '-XX:+UseStringDeduplication',
          '-XX:MaxGCPauseMillis=100',
          '-XX:InitiatingHeapOccupancyPercent=30',
          '-XX:+HeapDumpOnOutOfMemoryError',
          '-XX:ParallelGCThreads=4',
          '-Xlog:gc*,safepoint=info:file=' + self.log_dir + '/gc.' + gc_name +
          '.log:tags,time,uptime,level:filecount=5,filesize=100M']
    else:
      gc_cmd += [
          '-XX:+UseConcMarkSweepGC',
          '-XX:+CMSScavengeBeforeRemark',
          '-XX:TargetSurvivorRatio=90',
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
          '-XX:ParallelGCThreads=4',
          '-Xloggc:' + self.log_dir + '/gc.' + gc_name + '.log']
    try:
      cp_index = cmd.index('-cp')
      return list(itertools.chain(*[cmd[0:cp_index], gc_cmd, cmd[cp_index:]]))
    except ValueError:
      return cmd

  def _get_jvm_instance_options(self, instance_id, component_name, remote_debugger_port):
    code_cache_size_mb = 64
    java_metasize_mb = 128

    total_jvm_size = int(self.component_ram_map[component_name] / (1024 * 1024))
    heap_size_mb = total_jvm_size - code_cache_size_mb - java_metasize_mb
    Log.info("component name: %s, RAM request: %d, total JVM size: %dM, "
             "cache size: %dM, metaspace size: %dM"
             % (component_name, self.component_ram_map[component_name],
                total_jvm_size, code_cache_size_mb, java_metasize_mb))
    xmn_size = int(heap_size_mb / 2)

    java_version = self._get_jvm_version()
    java_metasize_param = 'MetaspaceSize'
    if java_version.startswith("1.7") or \
            java_version.startswith("1.6") or \
            java_version.startswith("1.5"):
      java_metasize_param = 'PermSize'

    instance_options = [
        '-Xmx%dM' % heap_size_mb,
        '-Xms%dM' % heap_size_mb,
        '-Xmn%dM' % xmn_size,
        '-XX:Max%s=%dM' % (java_metasize_param, java_metasize_mb),
        '-XX:%s=%dM' % (java_metasize_param, java_metasize_mb),
        '-XX:ReservedCodeCacheSize=%dM' % code_cache_size_mb,
        '-XX:+PrintCommandLineFlags',
        '-Djava.net.preferIPv4Stack=true',
        '-cp',
        '%s:%s'% (self.instance_classpath, self.classpath)]

    # Insert GC Options
    instance_options = self._get_java_gc_instance_cmd(instance_options, instance_id)
    # Append debugger ports when it is available
    if remote_debugger_port:
      instance_options.append('-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%s'
                              % remote_debugger_port)

    # Append user specified jvm options
    instance_options.extend(self.instance_jvm_opts.split())
    if component_name in self.component_jvm_opts:
      instance_options.extend(self.component_jvm_opts[component_name].split())

    return instance_options

  def _get_jvm_instance_arguments(self, instance_id, component_name, global_task_id,
                                  component_index, remote_debugger_port):
    instance_args = [
        '-topology_name', self.topology_name,
        '-topology_id', self.topology_id,
        '-instance_id', instance_id,
        '-component_name', component_name,
        '-task_id', str(global_task_id),
        '-component_index', str(component_index),
        '-stmgr_id', self.stmgr_ids[self.shard],
        '-stmgr_port', self.tmaster_controller_port,
        '-metricsmgr_port', self.metrics_manager_port,
        '-system_config_file', self.heron_internals_config_file,
        '-override_config_file', self.override_config_file]

    # Append debugger ports when it is available
    if remote_debugger_port:
      instance_args += ['-remote_debugger_port', remote_debugger_port]

    return instance_args

  def _get_jvm_version(self):
    if not self.jvm_version:
      cmd = [os.path.join(self.heron_java_home, 'bin/java'),
             '-cp', self.instance_classpath, 'org.apache.heron.instance.util.JvmVersion']
      process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      (process_stdout, process_stderr) = process.communicate()
      if process.returncode != 0:
        Log.error("Failed to determine JVM version. Exiting. Output of %s: %s",
                  ' '.join(cmd), process_stderr)
        sys.exit(1)

      self.jvm_version = process_stdout
      Log.info("Detected JVM version %s" % self.jvm_version)
    return self.jvm_version

  # Returns the processes for each Python Heron Instance
  def _get_python_instance_cmd(self, instance_info):
    # pylint: disable=fixme
    # TODO: currently ignoring ramsize, heap, etc.
    retval = {}
    for (instance_id, component_name, global_task_id, component_index) in instance_info:
      Log.info("Python instance %s component: %s" %(instance_id, component_name))
      instance_cmd = [self.python_instance_binary,
                      '--topology_name=%s' % self.topology_name,
                      '--topology_id=%s' % self.topology_id,
                      '--instance_id=%s' % instance_id,
                      '--component_name=%s' % component_name,
                      '--task_id=%s' % str(global_task_id),
                      '--component_index=%s' % str(component_index),
                      '--stmgr_id=%s' % self.stmgr_ids[self.shard],
                      '--stmgr_port=%s' % self.tmaster_controller_port,
                      '--metricsmgr_port=%s' % self.metrics_manager_port,
                      '--sys_config=%s' % self.heron_internals_config_file,
                      '--override_config=%s' % self.override_config_file,
                      '--topology_pex=%s' % self.topology_binary_file,
                      '--max_ram=%s' % str(self.component_ram_map[component_name])]

      retval[instance_id] = Command(instance_cmd, self.shell_env)

    return retval

  # Returns the processes for each CPP Heron Instance
  def _get_cpp_instance_cmd(self, instance_info):
    # pylint: disable=fixme
    # TODO: currently ignoring ramsize, heap, etc.
    retval = {}
    for (instance_id, component_name, global_task_id, component_index) in instance_info:
      Log.info("CPP instance %s component: %s" %(instance_id, component_name))
      instance_cmd = [
          self.cpp_instance_binary,
          '--topology_name=%s' % self.topology_name,
          '--topology_id=%s' % self.topology_id,
          '--instance_id=%s' % instance_id,
          '--component_name=%s' % component_name,
          '--task_id=%s' % str(global_task_id),
          '--component_index=%s' % str(component_index),
          '--stmgr_id=%s' % self.stmgr_ids[self.shard],
          '--stmgr_port=%s' % str(self.tmaster_controller_port),
          '--metricsmgr_port=%s' % str(self.metrics_manager_port),
          '--config_file=%s' % self.heron_internals_config_file,
          '--override_config_file=%s' % self.override_config_file,
          '--topology_binary=%s' % os.path.abspath(self.topology_binary_file)
      ]

      retval[instance_id] = Command(instance_cmd, self.shell_env)

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

    stmgr_cmd_lst = [
        self.stmgr_binary,
        '--topology_name=%s' % self.topology_name,
        '--topology_id=%s' % self.topology_id,
        '--topologydefn_file=%s' % self.topology_defn_file,
        '--zkhostportlist=%s' % self.state_manager_connection,
        '--zkroot=%s' % self.state_manager_root,
        '--stmgr_id=%s' % self.stmgr_ids[self.shard],
        '--instance_ids=%s' % ','.join([x[0] for x in instance_info]),
        '--myhost=%s' % self.master_host,
        '--data_port=%s' % str(self.master_port),
        '--local_data_port=%s' % str(self.tmaster_controller_port),
        '--metricsmgr_port=%s' % str(self.metrics_manager_port),
        '--shell_port=%s' % str(self.shell_port),
        '--config_file=%s' % self.heron_internals_config_file,
        '--override_config_file=%s' % self.override_config_file,
        '--ckptmgr_port=%s' % str(self.checkpoint_manager_port),
        '--ckptmgr_id=%s' % self.ckptmgr_ids[self.shard],
        '--metricscachemgr_mode=%s' % self.metricscache_manager_mode.lower()]

    stmgr_env = self.shell_env.copy() if self.shell_env is not None else {}
    stmgr_cmd = Command(stmgr_cmd_lst, stmgr_env)
    if os.environ.get('ENABLE_HEAPCHECK') is not None:
      stmgr_cmd.env.update({
          'LD_PRELOAD': "/usr/lib/libtcmalloc.so",
          'HEAPCHECK': "normal"
      })

    retval[self.stmgr_ids[self.shard]] = stmgr_cmd

    # metricsmgr_metrics_sink_config_file = 'metrics_sinks.yaml'

    retval[self.metricsmgr_ids[self.shard]] = self._get_metricsmgr_cmd(
        self.metricsmgr_ids[self.shard],
        self.metrics_sinks_config_file,
        self.metrics_manager_port
    )

    if self.is_stateful_topology:
      retval.update(self._get_ckptmgr_process())

    if self.pkg_type == 'jar' or self.pkg_type == 'tar':
      retval.update(self._get_java_instance_cmd(instance_info))
    elif self.pkg_type == 'pex':
      retval.update(self._get_python_instance_cmd(instance_info))
    elif self.pkg_type == 'so':
      retval.update(self._get_cpp_instance_cmd(instance_info))
    elif self.pkg_type == 'dylib':
      retval.update(self._get_cpp_instance_cmd(instance_info))
    else:
      raise ValueError("Unrecognized package type: %s" % self.pkg_type)

    return retval

  def _get_ckptmgr_process(self):
    ''' Get the command to start the checkpoint manager process'''

    ckptmgr_main_class = 'org.apache.heron.ckptmgr.CheckpointManager'

    ckptmgr_ram_mb = self.checkpoint_manager_ram / (1024 * 1024)
    ckptmgr_id = self.ckptmgr_ids[self.shard]
    ckptmgr_cmd = [os.path.join(self.heron_java_home, "bin/java"),
                   '-Xms%dM' % ckptmgr_ram_mb,
                   '-Xmx%dM' % ckptmgr_ram_mb,
                   '-XX:+PrintCommandLineFlags',
                   '-Djava.net.preferIPv4Stack=true',
                   '-cp',
                   self.checkpoint_manager_classpath,
                   ckptmgr_main_class,
                   '-t' + self.topology_name,
                   '-i' + self.topology_id,
                   '-c' + ckptmgr_id,
                   '-p' + self.checkpoint_manager_port,
                   '-f' + self.stateful_config_file,
                   '-o' + self.override_config_file,
                   '-g' + self.heron_internals_config_file]

    # Insert GC Options
    ckptmgr_cmd = self._get_java_gc_instance_cmd(ckptmgr_cmd, ckptmgr_id)
    retval = {}
    retval[self.ckptmgr_ids[self.shard]] = Command(ckptmgr_cmd, self.shell_env)

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

    # When the executor runs in newly added container by `heron update`,
    # there is no plan for this container. In this situation,
    # return None to bypass instance processes.
    if this_container_plan is None:
      return None
    return this_container_plan.instance_plans

  # Returns the common heron support processes that all containers get, like the heron shell
  def _get_heron_support_processes(self):
    """ Get a map from all daemon services' name to the command to start them """
    retval = {}

    retval[self.heron_shell_ids[self.shard]] = Command([
        '%s' % self.heron_shell_binary,
        '--port=%s' % self.shell_port,
        '--log_file_prefix=%s/heron-shell-%s.log' % (self.log_dir, self.shard),
        '--secret=%s' % self.topology_id], self.shell_env)

    return retval

  def _untar_if_needed(self):
    if self.pkg_type == "tar":
      os.system("tar -xvf %s" % self.topology_binary_file)
    elif self.pkg_type == "pex":
      os.system("unzip -qq -n %s" % self.topology_binary_file)

  # pylint: disable=no-self-use
  def _wait_process_std_out_err(self, name, process):
    ''' Wait for the termination of a process and log its stdout & stderr '''
    proc.stream_process_stdout(process, stdout_log_fn(name))
    process.wait()

  def _run_process(self, name, cmd):
    Log.info("Running %s process as %s" % (name, cmd))
    try:
      # stderr is redirected to stdout so that it can more easily be logged. stderr has a max buffer
      # size and can cause the child process to deadlock if it fills up
      process = subprocess.Popen(cmd.cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 env=cmd.env, bufsize=1)
      proc.async_stream_process_stdout(process, stdout_log_fn(name))
    except Exception:
      Log.info("Exception running command %s", cmd)
      traceback.print_exc()

    return process

  def _run_blocking_process(self, cmd, is_shell=False):
    Log.info("Running blocking process as %s" % cmd)
    try:
      # stderr is redirected to stdout so that it can more easily be logged. stderr has a max buffer
      # size and can cause the child process to deadlock if it fills up
      process = subprocess.Popen(cmd.cmd, shell=is_shell, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, env=cmd.env)

      # wait for termination
      self._wait_process_std_out_err(cmd.cmd, process)
    except Exception:
      Log.info("Exception running command %s", cmd)
      traceback.print_exc()

    # return the exit code
    return process.returncode

  def _kill_processes(self, commands):
    # remove the command from processes_to_monitor and kill the process
    with self.process_lock:
      for command_name, command in list(commands.items()):
        for process_info in list(self.processes_to_monitor.values()):
          if process_info.name == command_name:
            del self.processes_to_monitor[process_info.pid]
            Log.info("Killing %s process with pid %d: %s" %
                     (process_info.name, process_info.pid, command))
            try:
              process_info.process.terminate()  # sends SIGTERM to process
            except OSError as e:
              if e.errno == 3: # No such process
                Log.warn("Expected process %s with pid %d was not running, ignoring." %
                         (process_info.name, process_info.pid))
              else:
                raise e

  def _start_processes(self, commands):
    """Start all commands and add them to the dict of processes to be monitored """
    Log.info("Start processes")
    processes_to_monitor = {}
    # First start all the processes
    for (name, command) in list(commands.items()):
      p = self._run_process(name, command)
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
    Log.info("Start process monitor")
    while True:
      if len(self.processes_to_monitor) > 0:
        (pid, status) = os.wait()

        with self.process_lock:
          if pid in list(self.processes_to_monitor.keys()):
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
    """
    Prepare either TMaster or Streaming commands according to shard.
    The Shell command is attached to all containers. The empty container plan and non-exist
    container plan are bypassed.
    """
    # During shutdown the watch might get triggered with the empty packing plan
    if len(self.packing_plan.container_plans) == 0:
      return {}
    if self._get_instance_plans(self.packing_plan, self.shard) is None and self.shard != 0:
      retval = {}
      retval['heron-shell'] = Command([
          '%s' % self.heron_shell_binary,
          '--port=%s' % self.shell_port,
          '--log_file_prefix=%s/heron-shell-%s.log' % (self.log_dir, self.shard),
          '--secret=%s' % self.topology_id], self.shell_env)
      return retval

    if self.shard == 0:
      commands = self._get_tmaster_processes()
    else:
      self._untar_if_needed()
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
    for current_name, current_command in list(current_commands.items()):
      # We don't restart tmaster since it watches the packing plan and updates itself. The stream
      # manager is restarted just to reset state, but we could update it to do so without a restart
      if current_name in list(updated_commands.keys()) and \
        current_command == updated_commands[current_name] and \
        not current_name.startswith('stmgr-'):
        commands_to_keep[current_name] = current_command
      else:
        commands_to_kill[current_name] = current_command

    # updated commands not in the keep list need to be started
    for updated_name, updated_command in list(updated_commands.items()):
      if updated_name not in list(commands_to_keep.keys()):
        commands_to_start[updated_name] = updated_command

    return commands_to_kill, commands_to_keep, commands_to_start

  def launch(self):
    ''' Determines the commands to be run and compares them with the existing running commands.
    Then starts new ones required and kills old ones no longer required.
    '''
    with self.process_lock:
      current_commands = dict(list(map((lambda process: (process.name, process.command)),
                                       list(self.processes_to_monitor.values()))))
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
    Log.info("Start state manager watches")

    with open(self.override_config_file, 'r') as stream:
      overrides = yaml.load(stream)
      if overrides is None:
        overrides = dict()
    overrides["heron.statemgr.connection.string"] = self.state_manager_connection

    statemgr_config = StateMgrConfig()
    statemgr_config.set_state_locations(configloader.load_state_manager_locations(
        self.cluster, state_manager_config_file=self.state_manager_config_file,
        **overrides))
    try:
      self.state_managers = statemanagerfactory.get_all_state_managers(statemgr_config)
      for state_manager in self.state_managers:
        state_manager.start()
    except Exception as ex:
      Log.error("Found exception while initializing state managers: %s. Bailing out..." % ex)
      traceback.print_exc()
      sys.exit(1)

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
      onPackingPlanWatch = functools.partial(on_packing_plan_watch, state_manager)
      state_manager.get_packing_plan(self.topology_name, onPackingPlanWatch)
      Log.info("Registered state watch for packing plan changes with state manager %s." %
               str(state_manager))

  def stop_state_manager_watches(self):
    Log.info("Stopping state managers")
    for state_manager in self.state_managers:
      state_manager.stop()

def setup(executor):
  """Set up log, process and signal handlers"""
  # pylint: disable=unused-argument
  def signal_handler(signal_to_handle, frame):
    # We would do nothing here but just exit
    # Just catch the SIGTERM and then cleanup(), registered with atexit, would invoke
    Log.info('signal_handler invoked with signal %s', signal_to_handle)
    executor.stop_state_manager_watches()
    sys.exit(signal_to_handle)

  def cleanup():
    """Handler to trigger when receiving the SIGTERM signal
    Do cleanup inside this method, including:
    1. Terminate all children processes
    """
    Log.info('Executor terminated; exiting all process in executor.')

    # Kill child processes first and wait for log collection to finish
    for pid in list(executor.processes_to_monitor.keys()):
      os.kill(pid, signal.SIGTERM)
    time.sleep(5)

    # We would not wait or check whether process spawned dead or not
    os.killpg(0, signal.SIGTERM)

  # Redirect stdout and stderr to files in append mode
  # The filename format is heron-executor-<container_id>.stdxxx
  shardid = executor.shard
  log.configure(logfile='heron-executor-%s.stdout' % shardid)

  pid = os.getpid()
  sid = os.getsid(pid)

  # POSIX prohibits the change of the process group ID of a session leader
  if pid != sid:
    Log.info('Set up process group; executor becomes leader')
    os.setpgrp() # create new process group, become its leader

  Log.info('Register the SIGTERM signal handler')
  signal.signal(signal.SIGTERM, signal_handler)

  Log.info('Register the atexit clean up')
  atexit.register(cleanup)

def start(executor):
  """Set up environment and start executor"""
  setup(executor)

  # Start state manager watches which are responsible for monitoring states and
  # launch processes
  executor.start_state_manager_watches()

  # Start process monitor which are responsible for restarting processes when
  # they are dead. This is the main loop of executor
  executor.start_process_monitor()

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
  executor.initialize()

  start(executor)

if __name__ == "__main__":
  main()
