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
'''heron executor unittest'''
import os
import unittest2 as unittest

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

  return yaml_path

INTERNAL_CONF_PATH = get_test_heron_internal_yaml()

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

class HeronExecutorTest(unittest.TestCase):
  """Unittest for Heron Executor"""

  shell_command_expected = 'heron_shell_binary --port=shell-port ' \
                           '--log_file_prefix=fake_dir/heron-shell.log'

  def build_packing_plan(self, instance_distribution):
    packing_plan = PackingPlan()
    for container_id in instance_distribution.keys():
      container_plan = packing_plan.container_plans.add()
      container_plan.id = str(container_id)
      for (component_name, global_task_id, component_index) in instance_distribution[container_id]:
        instance_plan = container_plan.instance_plans.add()
        instance_plan.id = "%s:%s:%s:%s" %\
                           (container_id, component_name, global_task_id, component_index)
        instance_plan.component_name = component_name
    return packing_plan

  # pylint: disable=no-self-argument
  def get_expected_metricsmgr_command(container_id):
    return "heron_java_home/bin/java -Xmx1024M -XX:+PrintCommandLineFlags -verbosegc " \
           "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause " \
           "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC " \
           "-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+PrintCommandLineFlags " \
           "-Xloggc:log-files/gc.metricsmgr.log -Djava.net.preferIPv4Stack=true " \
           "-cp metricsmgr_classpath com.twitter.heron.metricsmgr.MetricsManager metricsmgr-%d " \
           "metricsmgr_port topname topid %s " \
           "metrics_sinks_config_file" % (container_id, INTERNAL_CONF_PATH)

  def get_expected_instance_command(component_name, instance_id, container_id=1):
    instance_name = "container_%d_%s_%d" % (container_id, component_name, instance_id)
    return "heron_java_home/bin/java -Xmx320M -Xms320M -Xmn160M -XX:MaxPermSize=128M " \
           "-XX:PermSize=128M -XX:ReservedCodeCacheSize=64M -XX:+CMSScavengeBeforeRemark " \
           "-XX:TargetSurvivorRatio=90 -XX:+PrintCommandLineFlags -verbosegc -XX:+PrintGCDetails " \
           "-XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCCause " \
           "-XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC " \
           "-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=4 " \
           "-Xloggc:log-files/gc.%s.log -XX:+HeapDumpOnOutOfMemoryError " \
           "-Djava.net.preferIPv4Stack=true -cp instance_classpath:classpath " \
           "com.twitter.heron.instance.HeronInstance topname topid %s %s %d 0 stmgr-%d " \
           "master_port metricsmgr_port %s" \
           % (instance_name, instance_name, component_name, instance_id,
              container_id, INTERNAL_CONF_PATH)

  MockPOpen.set_next_pid(37)
  expected_processes_container_0 = [
      ProcessInfo(MockPOpen(), 'heron-shell-0', shell_command_expected),
      ProcessInfo(MockPOpen(), 'metricsmgr-0', get_expected_metricsmgr_command(0)),
      ProcessInfo(MockPOpen(), 'heron-tmaster',
                  'tmaster_binary master_port '
                  'tmaster_controller_port tmaster_stats_port '
                  'topname topid zknode zkroot stmgr-1 '
                  '%s metrics_sinks_config_file metricsmgr_port' % INTERNAL_CONF_PATH),
  ]

  MockPOpen.set_next_pid(37)
  expected_processes_container_1 = [
      ProcessInfo(MockPOpen(), 'stmgr-1',
                  'stmgr_binary topname topid topdefnfile zknode zkroot stmgr-1 '
                  'container_1_word_3,container_1_exclaim1_2,container_1_exclaim1_1 master_port '
                  'metricsmgr_port shell-port %s' % INTERNAL_CONF_PATH),
      ProcessInfo(MockPOpen(), 'container_1_word_3', get_expected_instance_command('word', 3)),
      ProcessInfo(MockPOpen(), 'container_1_exclaim1_1',
                  get_expected_instance_command('exclaim1', 1)),
      ProcessInfo(MockPOpen(), 'container_1_exclaim1_2',
                  get_expected_instance_command('exclaim1', 2)),
      ProcessInfo(MockPOpen(), 'heron-shell-1', shell_command_expected),
      ProcessInfo(MockPOpen(), 'metricsmgr-1', get_expected_metricsmgr_command(1)),
  ]

  def setUp(self):
    MockPOpen.set_next_pid(37)
    self.maxDiff = None
    self.executor_0 = MockExecutor(self.get_args(0))
    self.executor_1 = MockExecutor(self.get_args(1))
    self.packing_plan_expected = self.build_packing_plan(
      {1:[('word', '3', '0'), ('exclaim1', '2', '0'), ('exclaim1', '1', '0')]})

  # ./heron-executor <shardid> <topname> <topid> <topdefnfile>
  # <instance_distribution> <zknode> <zkroot> <tmaster_binary> <stmgr_binary>
  # <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath>
  # <master_port> <tmaster_controller_port> <tmaster_stats_port> <heron_internals_config_file>
  # <component_rammap> <component_jvm_opts_in_base64> <pkg_type> <topology_bin_file>
  # <heron_java_home> <shell-port> <heron_shell_binary> <metricsmgr_port>
  # <cluster> <role> <environ> <instance_classpath> <metrics_sinks_config_file>
  # <scheduler_classpath> <scheduler_port> <python_instance_binary>
  @staticmethod
  def get_args(shard_id):
    return ("""
    ./heron-executor %d topname topid topdefnfile
    1:word:3:0:exclaim1:2:0:exclaim1:1:0 zknode zkroot tmaster_binary stmgr_binary
    metricsmgr_classpath "LVhYOitIZWFwRHVtcE9uT3V0T2ZNZW1vcnlFcnJvcg&equals;&equals;" classpath
    master_port tmaster_controller_port tmaster_stats_port
    %s exclaim1:536870912,word:536870912 "" jar topology_bin_file
    heron_java_home shell-port heron_shell_binary metricsmgr_port
    cluster role environ instance_classpath metrics_sinks_config_file
    scheduler_classpath scheduler_port python_instance_binary
    """ % (shard_id, INTERNAL_CONF_PATH)).replace("\n", '').split()

  def test_update_packing_plan(self):
    self.executor_0.update_packing_plan(self.packing_plan_expected)

    self.assertEquals(self.packing_plan_expected, self.executor_0.packing_plan)
    self.assertEquals(["stmgr-1"], self.executor_0.stmgr_ids)
    self.assertEquals(["metricsmgr-0", "metricsmgr-1"], self.executor_0.metricsmgr_ids)
    self.assertEquals(["heron-shell-0", "heron-shell-1"], self.executor_0.heron_shell_ids)

  def test_launch_container_0(self):
    self.do_test_launch(self.executor_0, self.expected_processes_container_0)

  def test_launch_container_1(self):
    self.do_test_launch(self.executor_1, self.expected_processes_container_1)

  def do_test_launch(self, executor, expected_processes):
    executor.update_packing_plan(self.packing_plan_expected)
    executor.launch()
    monitored_processes = executor.processes_to_monitor

    # convert to (pid, name, command)
    found_processes = map(lambda (process_info):
                          (process_info.pid, process_info.name, process_info.command_str),
                          executor.processes)
    found_monitored = map(lambda (pid, process_info):
                          (pid, process_info.name, process_info.command_str),
                          monitored_processes.items())
    print "do_test_commands - found_processes: %s found_monitored: %s" \
          % (found_processes, found_monitored)
    self.assertEquals(found_processes, found_monitored)

    print "do_test_commands - expected_processes: %s monitored_processes: %s" \
          % (expected_processes, monitored_processes)
    self.assert_processes(expected_processes, monitored_processes)

  def test_change_instance_dist_container_1(self):
    MockPOpen.set_next_pid(37)
    self.executor_1.update_packing_plan(self.packing_plan_expected)
    current_commands = self.executor_1.get_commands_to_run()

    self.assertEquals(dict(
        map((lambda (process_info): (process_info.name, process_info.command.split(' '))),
            self.expected_processes_container_1)), current_commands)

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
