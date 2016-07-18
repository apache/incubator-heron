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

import unittest2 as unittest

from heron.executor.src.python.heron_executor_core import *

class MockPOpen:
  next_pid = 0

  def __init__(self):
    self.pid = MockPOpen.next_pid
    MockPOpen.next_pid += 1

  @staticmethod
  def set_next_pid(next_pid):
    MockPOpen.next_pid = next_pid

class MockExecutor(HeronExecutor):
  def load_logging_dir(self, heron_internals_config_file):
    return "fake_dir"

  def run_process(self, name, cmd):
    return MockPOpen()

class HeronExecutorTest(unittest.TestCase):
  dist_expected = { 1:[('word', '3', '0'), ('exclaim1', '2', '0'), ('exclaim1', '1', '0')]}
  shell_command_expected = 'heron_shell_binary --port=shell-port --log_file_prefix=fake_dir/heron-shell.log'

  def get_expected_metricsmgr_command(container_id):
    return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+PrintCommandLineFlags -Xloggc:log-files/gc.metricsmgr.log -Djava.net.preferIPv4Stack=true -cp metricsmgr_classpath com.twitter.heron.metricsmgr.MetricsManager metricsmgr-%d metricsmgr_port topname topid heron/config/src/yaml/conf/test/test_heron_internals.yaml metrics_sinks_config_file" % container_id

  def get_expected_instance_command(component_name, instance_id, container_id):
    instance_name = "container_%d_%s_%d" % (container_id, component_name, instance_id)
    return "heron_java_home/bin/java -Xmx320M -Xms320M -Xmn160M -XX:MaxPermSize=128M -XX:PermSize=128M -XX:ReservedCodeCacheSize=64M -XX:+CMSScavengeBeforeRemark -XX:TargetSurvivorRatio=90 -XX:+PrintCommandLineFlags -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=4 -Xloggc:log-files/gc.%s.log -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true -cp instance_classpath:classpath com.twitter.heron.instance.HeronInstance topname topid %s %s %d 0 stmgr-%d master_port metricsmgr_port heron/config/src/yaml/conf/test/test_heron_internals.yaml" % (instance_name, instance_name, component_name, instance_id, container_id)

  expected_processes_container_0 = [
    (37, 'heron-shell-0', shell_command_expected),
    (38, 'metricsmgr-0',  get_expected_metricsmgr_command(0)),
    (39, 'heron-tmaster', 'tmaster_binary master_port tmaster_controller_port tmaster_stats_port topname topid zknode zkroot stmgr-1 heron/config/src/yaml/conf/test/test_heron_internals.yaml metrics_sinks_config_file metricsmgr_port'),
  ]

  expected_processes_container_1 = [
    (37, 'stmgr-1', 'stmgr_binary topname topid topdefnfile zknode zkroot stmgr-1 container_1_word_3,container_1_exclaim1_2,container_1_exclaim1_1 master_port metricsmgr_port shell-port heron/config/src/yaml/conf/test/test_heron_internals.yaml'),
    (38, 'container_1_word_3', get_expected_instance_command('word', 3, 1)),
    (39, 'container_1_exclaim1_1', get_expected_instance_command('exclaim1', 1, 1)),
    (40, 'container_1_exclaim1_2', get_expected_instance_command('exclaim1', 2, 1)),
    (41, 'heron-shell-1', shell_command_expected),
    (42, 'metricsmgr-1', get_expected_metricsmgr_command(1)),
  ]

  def setUp(self):
    MockPOpen.set_next_pid(37)
    self.maxDiff = None
    self.executor_0 = MockExecutor(self.get_args(0))
    self.executor_1 = MockExecutor(self.get_args(1))

  # ./heron-executor <shardid> <topname> <topid> <topdefnfile>
  # <instance_distribution> <zknode> <zkroot> <tmaster_binary> <stmgr_binary>
  # <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath>
  # <master_port> <tmaster_controller_port> <tmaster_stats_port> <heron_internals_config_file>
  # <component_rammap> <component_jvm_opts_in_base64> <pkg_type> <topology_jar_file>
  # <heron_java_home> <shell-port> <heron_shell_binary> <metricsmgr_port>
  # <cluster> <role> <environ> <instance_classpath> <metrics_sinks_config_file>
  # <scheduler_classpath> <scheduler_port>
  def get_args(self, shard_id):
    return ("""
    ./heron-executor %d topname topid topdefnfile
    instance_dist zknode zkroot tmaster_binary stmgr_binary
    metricsmgr_classpath "LVhYOitIZWFwRHVtcE9uT3V0T2ZNZW1vcnlFcnJvcg&equals;&equals;" classpath
    master_port tmaster_controller_port tmaster_stats_port heron/config/src/yaml/conf/test/test_heron_internals.yaml
    exclaim1:536870912,word:536870912 "" pkg_type topology_jar_file
    heron_java_home shell-port heron_shell_binary metricsmgr_port
    cluster role environ instance_classpath metrics_sinks_config_file
    scheduler_classpath scheduler_port
    """ % (shard_id)).replace("\n", '').split()

  def test_parse_instance_distribution(self):
    dist_found = self.executor_0.parse_instance_distribution("1:word:3:0:exclaim1:2:0:exclaim1:1:0")
    self.assertEquals(self.dist_expected, dist_found)

  def test_update_instance_distribution(self):
    self.executor_0.update_instance_distribution(self.dist_expected)

    self.assertEquals(self.dist_expected, self.executor_0.instance_distribution)
    self.assertEquals(["stmgr-1"], self.executor_0.stmgr_ids)
    self.assertEquals(["metricsmgr-0", "metricsmgr-1"], self.executor_0.metricsmgr_ids)
    self.assertEquals(["heron-shell-0", "heron-shell-1"], self.executor_0.heron_shell_ids)

  def test_commands_container_0(self):
    self.do_test_commands(self.executor_0, self.expected_processes_container_0)

  def test_commands_container_1(self):
    self.do_test_commands(self.executor_1, self.expected_processes_container_1)

  def do_test_commands(self, executor, expected_processes):
    executor.update_instance_distribution(self.dist_expected)
    executor.launch()
    found_processes = executor.processes_to_monitor

    print("do_test_commands - expected_processes: %s found_process: %s" % (expected_processes, found_processes))
    self.assert_processes(expected_processes, found_processes)

  def test_change_instance_dist_container_1(self):
    MockPOpen.set_next_pid(37)
    self.executor_1.update_instance_distribution(self.dist_expected)
    current_commands = self.executor_1.get_commands_to_run()

    self.assertEquals(dict(map((lambda x: (x[1], x[2].split(' '))), self.expected_processes_container_1)), current_commands)

    # update instance distribution
    new_distribution = self.executor_1.parse_instance_distribution("1:word:3:0:word:2:0:exclaim1:1:0")
    self.executor_1.update_instance_distribution(new_distribution)
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
    (pid, expected_name, expected_command) = expected_process
    self.assertTrue(found_processes[pid])
    self.assertEquals((expected_name, 1), (found_processes[pid][1], found_processes[pid][3]))
    self.assertEquals(expected_command, ' '.join(found_processes[pid][2]))
