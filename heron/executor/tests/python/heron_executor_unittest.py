#!/usr/bin/env python3
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
import argparse
import os
import socket
import unittest
import json

from pprint import pprint

from heron.executor.src.python.heron_executor import cli, HeronExecutor, ProcessInfo
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

class MockPOpen:
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
    return "log-files"

  def _run_process(self, name, cmd, env=None):
    popen = MockPOpen()
    self.processes.append(ProcessInfo(popen, name, cmd))
    return popen

  def _get_jvm_version(self):
    return "11.0.6"


class HeronExecutorTest(unittest.TestCase):
  """Unittest for Heron Executor"""

  def get_expected_shell_command(container_id):
    return 'heron_shell_binary --port=shell-port ' \
           '--log_file_prefix=log-files/heron-shell-%s.log ' \
           '--secret=topid' % container_id

  def build_packing_plan(self, instance_distribution):
    packing_plan = PackingPlan()
    for container_id in list(instance_distribution.keys()):
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
    return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags " \
           "-Djava.net.preferIPv4Stack=true " \
           "-XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:+UseStringDeduplication " \
           "-XX:MaxGCPauseMillis=100 -XX:InitiatingHeapOccupancyPercent=30 " \
           "-XX:ParallelGCThreads=4 " \
           "-cp metricsmgr_classpath org.apache.heron.metricsmgr.MetricsManager " \
           f"--id=metricsmgr-{int(container_id)} --port=metricsmgr_port " \
           "--topology=topname --cluster=cluster --role=role --environment=environ " \
           "--topology-id=topid " \
           f"--system-config-file={INTERNAL_CONF_PATH} --override-config-file={OVERRIDE_PATH} " \
           "--sink-config-file=metrics_sinks_config_file"

  def get_expected_metricscachemgr_command():
    return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags " \
           "-Djava.net.preferIPv4Stack=true " \
           "-XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:+UseStringDeduplication " \
           "-XX:MaxGCPauseMillis=100 -XX:InitiatingHeapOccupancyPercent=30 " \
           "-XX:ParallelGCThreads=4 " \
           "-cp metricscachemgr_classpath org.apache.heron.metricscachemgr.MetricsCacheManager " \
           "--metricscache_id metricscache-0 --server_port metricscachemgr_serverport " \
           "--stats_port metricscachemgr_statsport --topology_name topname --topology_id topid " \
           f"--system_config_file {INTERNAL_CONF_PATH} --override_config_file {OVERRIDE_PATH} " \
           "--sink_config_file metrics_sinks_config_file " \
           "--cluster cluster --role role --environment environ"

  def get_expected_healthmgr_command():
    return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags " \
           "-Djava.net.preferIPv4Stack=true " \
           "-XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:+UseStringDeduplication " \
           "-XX:MaxGCPauseMillis=100 -XX:InitiatingHeapOccupancyPercent=30 " \
           "-XX:ParallelGCThreads=4 " \
           "-cp scheduler_classpath:healthmgr_classpath " \
           "org.apache.heron.healthmgr.HealthManager --cluster cluster --role role " \
           "--environment environ --topology_name topname --metricsmgr_port metricsmgr_port"

  def get_expected_instance_command(component_name, instance_id, container_id):
    instance_name = f"container_{int(container_id)}_{component_name}_{int(instance_id)}"
    return "heron_java_home/bin/java -Xmx320M -Xms320M -XX:MaxMetaspaceSize=128M " \
           "-XX:MetaspaceSize=128M -XX:ReservedCodeCacheSize=64M -XX:+PrintCommandLineFlags " \
           "-Djava.net.preferIPv4Stack=true " \
           "-XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:+UseStringDeduplication " \
           "-XX:MaxGCPauseMillis=100 -XX:InitiatingHeapOccupancyPercent=30 " \
           "-XX:ParallelGCThreads=4 " \
           "-cp instance_classpath:classpath -XX:+HeapDumpOnOutOfMemoryError " \
           "org.apache.heron.instance.HeronInstance -topology_name topname -topology_id topid " \
           f"-instance_id {instance_name} -component_name {component_name} " \
           f"-task_id {int(instance_id)} -component_index 0 -stmgr_id stmgr-{int(container_id)} " \
           "-stmgr_port tmanager_controller_port -metricsmgr_port metricsmgr_port " \
           f"-system_config_file {INTERNAL_CONF_PATH} -override_config_file {OVERRIDE_PATH}"

  MockPOpen.set_next_pid(37)
  expected_processes_container_0 = [
      ProcessInfo(MockPOpen(), 'heron-tmanager',
                  'tmanager_binary --topology_name=topname --topology_id=topid '
                  f'--zkhostportlist=zknode --zkroot=zkroot --myhost={HOSTNAME} --server_port=server_port '
                  '--controller_port=tmanager_controller_port --stats_port=tmanager_stats_port '
                  f'--config_file={INTERNAL_CONF_PATH} --override_config_file={OVERRIDE_PATH} '
                  '--metrics_sinks_yaml=metrics_sinks_config_file '
                  '--metricsmgr_port=metricsmgr_port --ckptmgr_port=ckptmgr-port'),
      ProcessInfo(MockPOpen(), 'heron-metricscache', get_expected_metricscachemgr_command()),
      ProcessInfo(MockPOpen(), 'heron-healthmgr', get_expected_healthmgr_command()),
      ProcessInfo(MockPOpen(), 'metricsmgr-0', get_expected_metricsmgr_command(0)),
      ProcessInfo(MockPOpen(), 'heron-shell-0', get_expected_shell_command(0)),
  ]

  MockPOpen.set_next_pid(37)
  expected_processes_container_1 = [
      ProcessInfo(MockPOpen(), 'stmgr-1',
                  'stmgr_binary --topology_name=topname --topology_id=topid '
                  '--topologydefn_file=topdefnfile --zkhostportlist=zknode --zkroot=zkroot '
                  '--stmgr_id=stmgr-1 '
                  '--instance_ids=container_1_word_3,container_1_exclaim1_2,container_1_exclaim1_1 '
                  f'--myhost={HOSTNAME} --data_port=server_port '
                  '--local_data_port=tmanager_controller_port --metricsmgr_port=metricsmgr_port '
                  f'--shell_port=shell-port --config_file={INTERNAL_CONF_PATH} --override_config_file={OVERRIDE_PATH} '
                  '--ckptmgr_port=ckptmgr-port --ckptmgr_id=ckptmgr-1 '
                  '--metricscachemgr_mode=cluster'),
      ProcessInfo(MockPOpen(), 'metricsmgr-1', get_expected_metricsmgr_command(1)),
      ProcessInfo(MockPOpen(), 'container_1_word_3', get_expected_instance_command('word', 3, 1)),
      ProcessInfo(MockPOpen(), 'container_1_exclaim1_2',
                  get_expected_instance_command('exclaim1', 2, 1)),
      ProcessInfo(MockPOpen(), 'container_1_exclaim1_1',
                  get_expected_instance_command('exclaim1', 1, 1)),
      ProcessInfo(MockPOpen(), 'heron-shell-1', get_expected_shell_command(1)),
  ]

  MockPOpen.set_next_pid(37)
  expected_processes_container_7 = [
      ProcessInfo(MockPOpen(), 'stmgr-7',
                  'stmgr_binary --topology_name=topname --topology_id=topid '
                  '--topologydefn_file=topdefnfile --zkhostportlist=zknode --zkroot=zkroot '
                  '--stmgr_id=stmgr-7 '
                  f'--instance_ids=container_7_word_11,container_7_exclaim1_210 --myhost={HOSTNAME} '
                  '--data_port=server_port '
                  '--local_data_port=tmanager_controller_port --metricsmgr_port=metricsmgr_port '
                  f'--shell_port=shell-port --config_file={INTERNAL_CONF_PATH} --override_config_file={OVERRIDE_PATH} '
                  '--ckptmgr_port=ckptmgr-port --ckptmgr_id=ckptmgr-7 '
                  '--metricscachemgr_mode=cluster'),
      ProcessInfo(MockPOpen(), 'metricsmgr-7', get_expected_metricsmgr_command(7)),
      ProcessInfo(MockPOpen(), 'container_7_word_11', get_expected_instance_command('word', 11, 7)),
      ProcessInfo(MockPOpen(), 'container_7_exclaim1_210',
                  get_expected_instance_command('exclaim1', 210, 7)),
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
  # <zknode> <zkroot> <tmanager_binary> <stmgr_binary>
  # <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath>
  # <server_port> <tmanager_controller_port> <tmanager_stats_port> <heron_internals_config_file>
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
      ("--tmanager-binary", "tmanager_binary"),
      ("--stmgr-binary", "stmgr_binary"),
      ("--metrics-manager-classpath", "metricsmgr_classpath"),
      ("--instance-jvm-opts", "LVhYOitIZWFwRHVtcE9uT3V0T2ZNZW1vcnlFcnJvcg(61)(61)"),
      ("--classpath", "classpath"),
      ("--server-port", "server_port"),
      ("--tmanager-controller-port", "tmanager_controller_port"),
      ("--tmanager-stats-port", "tmanager_stats_port"),
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
      ("--metricscache-manager-server-port", "metricscachemgr_serverport"),
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

    args = [f"{k}={v}" for k, v in executor_args]
    ctx = cli.make_context('heron-executor', args)
    return argparse.Namespace(**ctx.params)

  def test_update_packing_plan(self):
    self.executor_0.update_packing_plan(self.packing_plan_expected)

    self.assertEqual(self.packing_plan_expected, self.executor_0.packing_plan)
    self.assertEqual({1: "stmgr-1", 7: "stmgr-7"}, self.executor_0.stmgr_ids)
    self.assertEqual(
      {0: "metricsmgr-0", 1: "metricsmgr-1", 7: "metricsmgr-7"}, self.executor_0.metricsmgr_ids)
    self.assertEqual(
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
    found_processes = list([(process_info.pid, process_info.name, process_info.command_str) for process_info in executor.processes])
    found_monitored = list([(pinfo[0], pinfo[1].name, pinfo[1].command_str) for pinfo in list(monitored_processes.items())])
    found_processes.sort(key=lambda tuple: tuple[0])
    found_monitored.sort(key=lambda tuple: tuple[0])
    print("found_processes:")
    pprint(found_processes)
    print("found_monitored:")
    pprint(found_monitored)
    self.assertEqual(found_processes, found_monitored)

    print("expected_processes:")
    pprint(expected_processes)
    print("monitored_processes:")
    pprint(monitored_processes)
    self.assert_processes(expected_processes, monitored_processes)

  def test_change_instance_dist_container_1(self):
    MockPOpen.set_next_pid(37)
    self.executor_1.update_packing_plan(self.packing_plan_expected)
    current_commands = self.executor_1.get_commands_to_run()

    temp_dict = dict(
        list(map((lambda process_info: (process_info.name, process_info.command.split(' '))),
            self.expected_processes_container_1)))

    current_json = json.dumps(current_commands, sort_keys=True, cls=CommandEncoder).split(' ')
    temp_json = json.dumps(temp_dict, sort_keys=True).split(' ')

    print(f"current_json: {current_json}")
    print(f"temp_json: {temp_json}")

    # better test error report
    for (s1, s2) in zip(current_json, temp_json):
      self.assertEqual(s1, s2)

    # update instance distribution
    new_packing_plan = self.build_packing_plan(
      {1:[('word', '3', '0'), ('word', '2', '0'), ('exclaim1', '1', '0')]})
    self.executor_1.update_packing_plan(new_packing_plan)
    updated_commands = self.executor_1.get_commands_to_run()

    # get the commands to kill, keep and start and verify
    commands_to_kill, commands_to_keep, commands_to_start = \
      self.executor_1.get_command_changes(current_commands, updated_commands)

    self.assertEqual(['container_1_exclaim1_2', 'stmgr-1'], sorted(commands_to_kill.keys()))
    self.assertEqual(
        ['container_1_exclaim1_1', 'container_1_word_3', 'heron-shell-1', 'metricsmgr-1'],
        sorted(commands_to_keep.keys()))
    self.assertEqual(['container_1_word_2', 'stmgr-1'], sorted(commands_to_start.keys()))

  def assert_processes(self, expected_processes, found_processes):
    self.assertEqual(len(expected_processes), len(found_processes))
    for expected_process in expected_processes:
      self.assert_process(expected_process, found_processes)

  def assert_process(self, expected_process, found_processes):
    pid = expected_process.pid
    self.assertTrue(found_processes[pid])
    self.assertEqual(expected_process.name, found_processes[pid].name)
    self.assertEqual(expected_process.command, found_processes[pid].command_str)
    self.assertEqual(1, found_processes[pid].attempts)

