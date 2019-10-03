#!/usr/bin/env python
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

'''heron executor unittest'''
import os
import socket
import unittest2 as unittest
import json

from heron.executor.src.python.heron_executor import ProcessInfo
from heron.executor.src.python.heron_executor import HeronExecutor
from heron.proto.packing_plan_pb2 import PackingPlan

# pylint: disable=unused-argument
# pylint: disable=missing-docstring

def get_test_heron_internal_yaml():
  """Get the path to test_heron_internal.yaml

  For example, __file__ would be
  /tmp/_bazel_heron/randgen_dir/heron/heron/executor/tests/python/heron_executor_unittest.py
  """
  heron_dir = '/'.join(__file__.split('/')[:-5])
  yaml_path = os.path.join(heron_dir, 'heron/config/src/yaml/conf/test/test_heron_internals.yaml')
  override_path = os.path.join(
    heron_dir, 'heron/config/src/yaml/conf/test/test_override.yaml')

  return yaml_path, override_path

INTERNAL_CONF_PATH, OVERRIDE_PATH = get_test_heron_internal_yaml()
HOSTNAME = socket.gethostname()

class CommandEncoder(json.JSONEncoder):
  """Customized JSONEncoder that works with Command object"""
  def default(self, o):
    return o.cmd

class MockPOpen(object):
  """fake subprocess.Popen object that we can use to mock processes and pids"""
  next_pid = 0

  def __init__(self):
    self.pid = MockPOpen.next_pid
    MockPOpen.next_pid += 1

  @staticmethod
  def set_next_pid(next_pid):
    MockPOpen.next_pid = next_pid

class MockExecutor(HeronExecutor):
  """mock executor that overrides methods that don't apply to unit tests, like running processes"""
  def __init__(self, args):
    self.processes = []
    super(MockExecutor, self).__init__(args, None)

  # pylint: disable=no-self-use
  def _load_logging_dir(self, heron_internals_config_file):
    return "fake_dir"

  def _run_process(self, name, cmd, env=None):
    popen = MockPOpen()
    self.processes.append(ProcessInfo(popen, name, cmd))
    return popen

  def _get_jvm_version(self):
    return "1.8.y.x"

class HeronExecutorTest(unittest.TestCase):
  """Unittest for Heron Executor"""

  def get_expected_shell_command(container_id):
    return 'heron_shell_binary --port=shell-port ' \
           '--log_file_prefix=fake_dir/heron-shell-%s.log ' \
           '--secret=topid' % container_id

  def build_packing_plan(self, instance_distribution):
    packing_plan = PackingPlan()
    for container_id in instance_distribution.keys():
      container_plan = packing_plan.container_plans.add()
      container_plan.id = int(container_id)
      for (component_name, global_task_id, component_index) in instance_distribution[container_id]:
        instance_plan = container_plan.instance_plans.add()
        instance_plan.component_name = component_name
        instance_plan.task_id = int(global_task_id)
        instance_plan.component_index = int(component_index)
    return packing_plan

  # pylint: disable=no-self-argument
  def get_expected_metricsmgr_command(container_id):
    return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags -verbosegc " \
           "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause " \
           "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100M " \
           "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC " \
           "-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+PrintCommandLineFlags " \
           "-Xloggc:log-files/gc.metricsmgr-%d.log -Djava.net.preferIPv4Stack=true " \
           "-cp metricsmgr_classpath org.apache.heron.metricsmgr.MetricsManager " \
           "--id=metricsmgr-%d --port=metricsmgr_port " \
           "--topology=topname --cluster=cluster --role=role --environment=environ --topology-id=topid " \
           "--system-config-file=%s --override-config-file=%s --sink-config-file=metrics_sinks_config_file" %\
           (container_id, container_id, INTERNAL_CONF_PATH, OVERRIDE_PATH)

  def get_expected_metricscachemgr_command():
      return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags -verbosegc " \
             "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause " \
             "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100M " \
             "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC " \
             "-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+PrintCommandLineFlags " \
             "-Xloggc:log-files/gc.metricscache.log -Djava.net.preferIPv4Stack=true " \
             "-cp metricscachemgr_classpath org.apache.heron.metricscachemgr.MetricsCacheManager " \
             "--metricscache_id metricscache-0 --master_port metricscachemgr_masterport " \
             "--stats_port metricscachemgr_statsport --topology_name topname --topology_id topid " \
             "--system_config_file %s --override_config_file %s " \
             "--sink_config_file metrics_sinks_config_file " \
             "--cluster cluster --role role --environment environ" %\
             (INTERNAL_CONF_PATH, OVERRIDE_PATH)

  def get_expected_healthmgr_command():
      return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags -verbosegc " \
             "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause " \
             "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100M " \
             "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC " \
             "-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+PrintCommandLineFlags " \
             "-Xloggc:log-files/gc.healthmgr.log -Djava.net.preferIPv4Stack=true " \
             "-cp scheduler_classpath:healthmgr_classpath " \
             "org.apache.heron.healthmgr.HealthManager --cluster cluster --role role " \
             "--environment environ --topology_name topname --metricsmgr_port metricsmgr_port"

  def get_expected_instance_command(component_name, instance_id, container_id):
    instance_name = "container_%d_%s_%d" % (container_id, component_name, instance_id)
    return "heron_java_home/bin/java -Xmx320M -Xms320M -Xmn160M -XX:MaxMetaspaceSize=128M " \
           "-XX:MetaspaceSize=128M -XX:ReservedCodeCacheSize=64M -XX:+CMSScavengeBeforeRemark " \
           "-XX:TargetSurvivorRatio=90 -XX:+PrintCommandLineFlags -verbosegc -XX:+PrintGCDetails " \
           "-XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause " \
           "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100M " \
           "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC " \
           "-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=4 " \
           "-Xloggc:log-files/gc.%s.log -Djava.net.preferIPv4Stack=true " \
           "-cp instance_classpath:classpath -XX:+HeapDumpOnOutOfMemoryError " \
           "org.apache.heron.instance.HeronInstance -topology_name topname -topology_id topid -instance_id %s -component_name %s -task_id %d -component_index 0 -stmgr_id stmgr-%d " \
           "-stmgr_port tmaster_controller_port -metricsmgr_port metricsmgr_port -system_config_file %s -override_config_file %s" \
           % (instance_name, instance_name, component_name, instance_id,
              container_id, INTERNAL_CONF_PATH, OVERRIDE_PATH)

  MockPOpen.set_next_pid(37)
  expected_processes_container_0 = [
      ProcessInfo(MockPOpen(), 'heron-tmaster',
                  'tmaster_binary --topology_name=topname --topology_id=topid '
                  '--zkhostportlist=zknode --zkroot=zkroot --myhost=%s --master_port=master_port '
                  '--controller_port=tmaster_controller_port --stats_port=tmaster_stats_port '
                  '--config_file=%s --override_config_file=%s '
                  '--metrics_sinks_yaml=metrics_sinks_config_file '
                  '--metricsmgr_port=metricsmgr_port '
                  '--ckptmgr_port=ckptmgr-port' % (HOSTNAME, INTERNAL_CONF_PATH, OVERRIDE_PATH)),
      ProcessInfo(MockPOpen(), 'heron-shell-0', get_expected_shell_command(0)),
      ProcessInfo(MockPOpen(), 'metricsmgr-0', get_expected_metricsmgr_command(0)),
      ProcessInfo(MockPOpen(), 'heron-metricscache', get_expected_metricscachemgr_command()),
      ProcessInfo(MockPOpen(), 'heron-healthmgr', get_expected_healthmgr_command()),
  ]

  MockPOpen.set_next_pid(37)
  expected_processes_container_1 = [
      ProcessInfo(MockPOpen(), 'stmgr-1',
                  'stmgr_binary --topology_name=topname --topology_id=topid '
                  '--topologydefn_file=topdefnfile --zkhostportlist=zknode --zkroot=zkroot '
                  '--stmgr_id=stmgr-1 '
                  '--instance_ids=container_1_word_3,container_1_exclaim1_2,container_1_exclaim1_1 '
                  '--myhost=%s --data_port=master_port '
                  '--local_data_port=tmaster_controller_port --metricsmgr_port=metricsmgr_port '
                  '--shell_port=shell-port --config_file=%s --override_config_file=%s '
                  '--ckptmgr_port=ckptmgr-port --ckptmgr_id=ckptmgr-1 '
                  '--metricscachemgr_mode=cluster'
                  % (HOSTNAME, INTERNAL_CONF_PATH, OVERRIDE_PATH)),
      ProcessInfo(MockPOpen(), 'container_1_word_3', get_expected_instance_command('word', 3, 1)),
      ProcessInfo(MockPOpen(), 'container_1_exclaim1_1',
                  get_expected_instance_command('exclaim1', 1, 1)),
      ProcessInfo(MockPOpen(), 'container_1_exclaim1_2',
                  get_expected_instance_command('exclaim1', 2, 1)),
      ProcessInfo(MockPOpen(), 'heron-shell-1', get_expected_shell_command(1)),
      ProcessInfo(MockPOpen(), 'metricsmgr-1', get_expected_metricsmgr_command(1)),
  ]

  MockPOpen.set_next_pid(37)
  expected_processes_container_7 = [
      ProcessInfo(MockPOpen(), 'container_7_word_11', get_expected_instance_command('word', 11, 7)),
      ProcessInfo(MockPOpen(), 'container_7_exclaim1_210',
                  get_expected_instance_command('exclaim1', 210, 7)),
      ProcessInfo(MockPOpen(), 'stmgr-7',
                  'stmgr_binary --topology_name=topname --topology_id=topid '
                  '--topologydefn_file=topdefnfile --zkhostportlist=zknode --zkroot=zkroot '
                  '--stmgr_id=stmgr-7 '
                  '--instance_ids=container_7_word_11,container_7_exclaim1_210 --myhost=%s '
                  '--data_port=master_port '
                  '--local_data_port=tmaster_controller_port --metricsmgr_port=metricsmgr_port '
                  '--shell_port=shell-port --config_file=%s --override_config_file=%s '
                  '--ckptmgr_port=ckptmgr-port --ckptmgr_id=ckptmgr-7 '
                  '--metricscachemgr_mode=cluster'
                  % (HOSTNAME, INTERNAL_CONF_PATH, OVERRIDE_PATH)),
      ProcessInfo(MockPOpen(), 'metricsmgr-7', get_expected_metricsmgr_command(7)),
      ProcessInfo(MockPOpen(), 'heron-shell-7', get_expected_shell_command(7)),
  ]

  def setUp(self):
    MockPOpen.set_next_pid(37)
    self.maxDiff = None
    self.executor_0 = MockExecutor(self.get_args(0))
    self.executor_1 = MockExecutor(self.get_args(1))
    self.executor_7 = MockExecutor(self.get_args(7))
    self.packing_plan_expected = self.build_packing_plan({
      1:[('word', '3', '0'), ('exclaim1', '2', '0'), ('exclaim1', '1', '0')],
      7:[('word', '11', '0'), ('exclaim1', '210', '0')],
    })

  # ./heron-executor <shardid> <topname> <topid> <topdefnfile>
  # <zknode> <zkroot> <tmaster_binary> <stmgr_binary>
  # <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath>
  # <master_port> <tmaster_controller_port> <tmaster_stats_port> <heron_internals_config_file>
  # <override_config_file> <component_rammap> <component_jvm_opts_in_base64> <pkg_type>
  # <topology_bin_file> <heron_java_home> <shell-port> <heron_shell_binary> <metricsmgr_port>
  # <cluster> <role> <environ> <instance_classpath> <metrics_sinks_config_file>
  # <scheduler_classpath> <scheduler_port> <python_instance_binary>
  @staticmethod
  def get_args(shard_id):
    executor_args = [
      ("--shard", shard_id),
      ("--topology-name", "topname"),
      ("--topology-id", "topid"),
      ("--topology-defn-file", "topdefnfile"),
      ("--state-manager-connection", "zknode"),
      ("--state-manager-root", "zkroot"),
      ("--state-manager-config-file", "state_manager_config_file"),
      ("--tmaster-binary", "tmaster_binary"),
      ("--stmgr-binary", "stmgr_binary"),
      ("--metrics-manager-classpath", "metricsmgr_classpath"),
      ("--instance-jvm-opts", "LVhYOitIZWFwRHVtcE9uT3V0T2ZNZW1vcnlFcnJvcg(61)(61)"),
      ("--classpath", "classpath"),
      ("--master-port", "master_port"),
      ("--tmaster-controller-port", "tmaster_controller_port"),
      ("--tmaster-stats-port", "tmaster_stats_port"),
      ("--heron-internals-config-file", INTERNAL_CONF_PATH),
      ("--override-config-file", OVERRIDE_PATH),
      ("--component-ram-map", "exclaim1:536870912,word:536870912"),
      ("--component-jvm-opts", ""),
      ("--pkg-type", "jar"),
      ("--topology-binary-file", "topology_bin_file"),
      ("--heron-java-home", "heron_java_home"),
      ("--shell-port", "shell-port"),
      ("--heron-shell-binary", "heron_shell_binary"),
      ("--metrics-manager-port", "metricsmgr_port"),
      ("--cluster", "cluster"),
      ("--role", "role"),
      ("--environment", "environ"),
      ("--instance-classpath", "instance_classpath"),
      ("--metrics-sinks-config-file", "metrics_sinks_config_file"),
      ("--scheduler-classpath", "scheduler_classpath"),
      ("--scheduler-port", "scheduler_port"),
      ("--python-instance-binary", "python_instance_binary"),
      ("--cpp-instance-binary", "cpp_instance_binary"),
      ("--metricscache-manager-classpath", "metricscachemgr_classpath"),
      ("--metricscache-manager-master-port", "metricscachemgr_masterport"),
      ("--metricscache-manager-stats-port", "metricscachemgr_statsport"),
      ("--is-stateful", "is_stateful_enabled"),
      ("--checkpoint-manager-classpath", "ckptmgr_classpath"),
      ("--checkpoint-manager-port", "ckptmgr-port"),
      ("--checkpoint-manager-ram", "1073741824"),
      ("--stateful-config-file", "stateful_config_file"),
      ("--health-manager-mode", "cluster"),
      ("--health-manager-classpath", "healthmgr_classpath"),
      ("--metricscache-manager-mode", "cluster")
    ]

    args = ("%s=%s" % (arg[0], (str(arg[1]))) for arg in executor_args)
    command = "./heron-executor %s" % (" ".join(args))
    return command.split()


  def test_update_packing_plan(self):
    self.executor_0.update_packing_plan(self.packing_plan_expected)

    self.assertEquals(self.packing_plan_expected, self.executor_0.packing_plan)
    self.assertEquals({1: "stmgr-1", 7: "stmgr-7"}, self.executor_0.stmgr_ids)
    self.assertEquals(
      {0: "metricsmgr-0", 1: "metricsmgr-1", 7: "metricsmgr-7"}, self.executor_0.metricsmgr_ids)
    self.assertEquals(
      {0: "heron-shell-0", 1: "heron-shell-1", 7: "heron-shell-7"}, self.executor_0.heron_shell_ids)

  def test_launch_container_0(self):
    self.do_test_launch(self.executor_0, self.expected_processes_container_0)

  def test_launch_container_1(self):
    self.do_test_launch(self.executor_1, self.expected_processes_container_1)

  def test_launch_container_7(self):
    self.do_test_launch(self.executor_7, self.expected_processes_container_7)

  def do_test_launch(self, executor, expected_processes):
    executor.update_packing_plan(self.packing_plan_expected)
    executor.launch()
    monitored_processes = executor.processes_to_monitor

    # convert to (pid, name, command)
    found_processes = list(map(lambda process_info:
                          (process_info.pid, process_info.name, process_info.command_str),
                          executor.processes))
    found_monitored = list(map(lambda pinfo:
                          (pinfo[0], pinfo[1].name, pinfo[1].command_str),
                          monitored_processes.items()))
    found_processes.sort(key=lambda tuple: tuple[0])
    found_monitored.sort(key=lambda tuple: tuple[0])
    print("do_test_commands - found_processes: %s found_monitored: %s" \
          % (found_processes, found_monitored))
    self.assertEquals(found_processes, found_monitored)

    print("do_test_commands - expected_processes: %s monitored_processes: %s" \
          % (expected_processes, monitored_processes))
    self.assert_processes(expected_processes, monitored_processes)

  def test_change_instance_dist_container_1(self):
    MockPOpen.set_next_pid(37)
    self.executor_1.update_packing_plan(self.packing_plan_expected)
    current_commands = self.executor_1.get_commands_to_run()

    temp_dict = dict(
        map((lambda process_info: (process_info.name, process_info.command.split(' '))),
            self.expected_processes_container_1))

    current_json = json.dumps(current_commands, sort_keys=True, cls=CommandEncoder).split(' ')
    temp_json = json.dumps(temp_dict, sort_keys=True).split(' ')

    print ("current_json: %s" % current_json)
    print ("temp_json: %s" % temp_json)

    # better test error report
    for (s1, s2) in zip(current_json, temp_json):
      self.assertEquals(s1, s2)

    # update instance distribution
    new_packing_plan = self.build_packing_plan(
      {1:[('word', '3', '0'), ('word', '2', '0'), ('exclaim1', '1', '0')]})
    self.executor_1.update_packing_plan(new_packing_plan)
    updated_commands = self.executor_1.get_commands_to_run()

    # get the commands to kill, keep and start and verify
    commands_to_kill, commands_to_keep, commands_to_start = \
      self.executor_1.get_command_changes(current_commands, updated_commands)

    self.assertEquals(['container_1_exclaim1_2', 'stmgr-1'], sorted(commands_to_kill.keys()))
    self.assertEquals(
        ['container_1_exclaim1_1', 'container_1_word_3', 'heron-shell-1', 'metricsmgr-1'],
        sorted(commands_to_keep.keys()))
    self.assertEquals(['container_1_word_2', 'stmgr-1'], sorted(commands_to_start.keys()))

  def assert_processes(self, expected_processes, found_processes):
    self.assertEquals(len(expected_processes), len(found_processes))
    for expected_process in expected_processes:
      self.assert_process(expected_process, found_processes)

  def assert_process(self, expected_process, found_processes):
    pid = expected_process.pid
    self.assertTrue(found_processes[pid])
    self.assertEquals(expected_process.name, found_processes[pid].name)
    self.assertEquals(expected_process.command, found_processes[pid].command_str)
    self.assertEquals(1, found_processes[pid].attempts)
