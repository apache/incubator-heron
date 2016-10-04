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
'''module for single-thread Heron Instance in python'''
import collections
import logging
import os
import sys
import traceback
import signal
import yaml

from heron.common.src.python.basics import GatewayLooper
from heron.common.src.python.config import system_config
from heron.common.src.python.utils import log
from heron.common.src.python.utils.metrics import GatewayMetrics, PyMetrics, MetricsCollector
from heron.common.src.python.utils.misc import HeronCommunicator
from heron.common.src.python.network import create_socket_options

from heron.proto import physical_plan_pb2, tuple_pb2
from heron.instance.src.python.network import MetricsManagerClient, SingleThreadStmgrClient
from heron.instance.src.python.basics import SpoutInstance, BoltInstance

import heron.common.src.python.constants as constants

Log = log.Log
AssignedInstance = collections.namedtuple('AssignedInstance', 'is_spout, protobuf, py_class')

# pylint: disable=too-many-instance-attributes
class SingleThreadHeronInstance(object):
  """SingleThreadHeronInstance is an implementation of Heron Instance in python"""
  STREAM_MGR_HOST = "127.0.0.1"
  METRICS_MGR_HOST = "127.0.0.1"
  def __init__(self, topology_name, topology_id, instance,
               stream_port, metrics_port, topo_pex_file_path):
    # Basic information about this heron instance
    self.topology_name = topology_name
    self.topology_id = topology_id
    self.instance = instance
    self.stream_port = stream_port
    self.metrics_port = metrics_port
    self.topo_pex_file_abs_path = os.path.abspath(topo_pex_file_path)
    self.sys_config = system_config.get_sys_config()

    self.in_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)
    self.out_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)

    self.socket_map = dict()
    self.looper = GatewayLooper(self.socket_map)

    # Initialize metrics related
    self.out_metrics = HeronCommunicator()
    self.out_metrics.\
      register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_METRICS_WRITE_QUEUE_CAPACITY])
    self.metrics_collector = MetricsCollector(self.looper, self.out_metrics)
    self.gateway_metrics = GatewayMetrics(self.metrics_collector)
    self.py_metrics = PyMetrics(self.metrics_collector)

    # Create socket options and socket clients
    socket_options = create_socket_options()
    self._stmgr_client = \
      SingleThreadStmgrClient(self.looper, self, self.STREAM_MGR_HOST, stream_port,
                              topology_name, topology_id, instance, self.socket_map,
                              self.gateway_metrics, socket_options)
    self._metrics_client = \
      MetricsManagerClient(self.looper, self.METRICS_MGR_HOST, metrics_port, instance,
                           self.out_metrics, self.in_stream, self.out_stream,
                           self.socket_map, socket_options, self.gateway_metrics, self.py_metrics)
    self.my_pplan_helper = None

    # my_instance is a AssignedInstance tuple
    self.my_instance = None
    self.is_instance_started = False

    # Debugging purposes
    def go_trace(_, stack):
      with open("/tmp/trace.log", "w") as f:
        traceback.print_stack(stack, file=f)
      self.looper.register_timer_task_in_sec(self.looper.exit_loop, 0.0)
    signal.signal(signal.SIGUSR1, go_trace)

  def start(self):
    self._stmgr_client.start_connect()
    self._metrics_client.start_connect()
    # call send_buffered_messages every time it is waken up
    self.looper.add_wakeup_task(self.send_buffered_messages)
    self.looper.loop()

  def handle_new_tuple_set(self, tuple_msg_set):
    """Called when new TupleMessage arrives

    :param tuple_msg_set: HeronTupleSet type
    """
    if self.my_pplan_helper is None or self.my_instance is None:
      Log.error("Got tuple set when no instance assigned yet")
    else:
      # First add message to the in_stream
      self.in_stream.offer(tuple_msg_set)
      if self.my_pplan_helper.is_topology_running():
        self.my_instance.py_class.process_incoming_tuples()

  def handle_new_tuple_set_2(self, hts2):
    """Called when new HeronTupleSet2 arrives
       Convert(Assemble) HeronTupleSet2(raw byte array) to HeronTupleSet
       See more at GitHub PR #1421
    :param tuple_msg_set: HeronTupleSet2 type
    """
    if self.my_pplan_helper is None or self.my_instance is None:
      Log.error("Got tuple set when no instance assigned yet")
    else:
      hts = tuple_pb2.HeronTupleSet()
      if hts2.HasField('control'):
        hts.control.CopyFrom(hts2.control)
      else:
        hdts = tuple_pb2.HeronDataTupleSet()
        hdts.stream.CopyFrom(hts2.data.stream)
        try:
          for trunk in hts2.data.tuples:
            added_tuple = hdts.tuples.add()
            added_tuple.ParseFromString(trunk)
        except Exception:
          Log.exception('Fail to deserialize HeronDataTuple')
        hts.data.CopyFrom(hdts)
      self.in_stream.offer(hts)
      if self.my_pplan_helper.is_topology_running():
        self.my_instance.py_class.process_incoming_tuples()

  def send_buffered_messages(self):
    """Send messages in out_stream to the Stream Manager"""
    while not self.out_stream.is_empty():
      tuple_set = self.out_stream.poll()
      self.gateway_metrics.update_sent_packet(tuple_set.ByteSize())
      self._stmgr_client.send_message(tuple_set)

  def handle_state_change_msg(self, new_helper):
    """Called when state change is commanded by stream manager"""
    assert self.my_pplan_helper is not None
    assert self.my_instance is not None and self.my_instance.py_class is not None

    if self.my_pplan_helper.get_topology_state() != new_helper.get_topology_state():
      # handle state change
      if new_helper.is_topology_running():
        if not self.is_instance_started:
          self.start_instance()
        self.my_instance.py_class.invoke_activate()
      elif new_helper.is_topology_paused():
        self.my_instance.py_class.invoke_deactivate()
      else:
        raise RuntimeError("Unexpected TopologyState update: %s" % new_helper.get_topology_state())
    else:
      Log.info("Topology state remains the same.")

    # update the pplan_helper
    self.my_pplan_helper = new_helper

  def handle_assignment_msg(self, pplan_helper):
    """Called when new NewInstanceAssignmentMessage arrives

    Tells this instance to become either spout/bolt.

    :param pplan_helper: PhysicalPlanHelper class to become
    """

    self.my_pplan_helper = pplan_helper
    self.my_pplan_helper.set_topology_context(self.metrics_collector)

    if pplan_helper.is_spout:
      # Starting a spout
      my_spout = pplan_helper.get_my_spout()
      Log.info("Incarnating ourselves as spout: %s with task id %s",
               pplan_helper.my_component_name, str(pplan_helper.my_task_id))

      self.in_stream. \
        register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY])
      self.out_stream. \
        register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY])

      py_spout_instance = SpoutInstance(self.my_pplan_helper, self.in_stream, self.out_stream,
                                        self.looper)
      self.my_instance = AssignedInstance(is_spout=True,
                                          protobuf=my_spout,
                                          py_class=py_spout_instance)
    else:
      # Starting a bolt
      my_bolt = pplan_helper.get_my_bolt()
      Log.info("Incarnating ourselves as bolt: %s with task id %s",
               pplan_helper.my_component_name, str(pplan_helper.my_task_id))

      self.in_stream. \
        register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY])
      self.out_stream. \
        register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY])

      py_bolt_instance = BoltInstance(self.my_pplan_helper, self.in_stream, self.out_stream,
                                      self.looper)
      self.my_instance = AssignedInstance(is_spout=False,
                                          protobuf=my_bolt,
                                          py_class=py_bolt_instance)

    if pplan_helper.is_topology_running():
      try:
        self.start_instance()
      except Exception as e:
        Log.error("Error with starting bolt/spout instance: " + e.message)
        Log.error(traceback.format_exc())
    else:
      Log.info("The instance is deployed in deactivated state")

  def start_instance(self):
    try:
      Log.info("Starting bolt/spout instance now...")
      self.my_instance.py_class.start()
      self.is_instance_started = True
      Log.info("Started instance successfully.")
    except Exception as e:
      Log.error(traceback.format_exc())
      Log.error("Error when starting bolt/spout, bailing out...: %s", e.message)
      self.looper.exit_loop()

def print_usage(argv0):
  print("Usage: %s <topology_name> <topology_id> "
        "<instance_id> <component_name> <task_id> "
        "<component_index> <stmgr_id> <stmgr_port> <metricsmgr_port> "
        "<heron_internals_config_filename> <topology_pex_file_path>" % argv0)

def yaml_config_reader(config_path):
  """Reads yaml config file and returns auto-typed config_dict"""
  if not config_path.endswith(".yaml"):
    raise ValueError("Config file not yaml")

  with open(config_path, 'r') as f:
    config = yaml.load(f)

  return config

# pylint: disable=missing-docstring
def main():
  if len(sys.argv) != 12:
    print_usage(sys.argv[0])
    sys.exit(1)

  topology_name = sys.argv[1]
  topology_id = sys.argv[2]
  instance_id = sys.argv[3]
  component_name = sys.argv[4]
  task_id = sys.argv[5]
  component_index = sys.argv[6]
  stmgr_id = sys.argv[7]
  stmgr_port = sys.argv[8]
  metrics_port = sys.argv[9]
  sys_config = yaml_config_reader(sys.argv[10])
  topology_pex_file_path = sys.argv[11]

  system_config.set_sys_config(sys_config)

  # create the protobuf instance
  instance_info = physical_plan_pb2.InstanceInfo()
  instance_info.task_id = int(task_id)
  instance_info.component_index = int(component_index)
  instance_info.component_name = component_name

  instance = physical_plan_pb2.Instance()
  instance.instance_id = instance_id
  instance.stmgr_id = stmgr_id
  instance.info.MergeFrom(instance_info)

  # Logging init
  log_dir = os.path.abspath(sys_config[constants.HERON_LOGGING_DIRECTORY])
  max_log_files = sys_config[constants.HERON_LOGGING_MAXIMUM_FILES]
  max_log_bytes = sys_config[constants.HERON_LOGGING_MAXIMUM_SIZE_MB] * constants.MB

  log_file = os.path.join(log_dir, instance_id + ".log.0")
  log.init_rotating_logger(level=logging.INFO, logfile=log_file,
                           max_files=max_log_files, max_bytes=max_log_bytes)

  Log.info("\nStarting instance: " + instance_id + " for topology: " + topology_name +
           " and topologyId: " + topology_id + " for component: " + component_name +
           " with taskId: " + task_id + " and componentIndex: " + component_index +
           " and stmgrId: " + stmgr_id + " and stmgrPort: " + stmgr_port +
           " and metricsManagerPort: " + metrics_port +
           "\n **Topology Pex file located at: " + topology_pex_file_path)
  Log.debug("System config: " + str(sys_config))

  heron_instance = SingleThreadHeronInstance(topology_name, topology_id, instance, stmgr_port,
                                             metrics_port, topology_pex_file_path)
  heron_instance.start()

if __name__ == '__main__':
  main()
