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
import resource
import sys
import traceback
import signal
import yaml

import heronpy.api.api_constants as api_constants
from heronpy.api.state.state import HashMapState

from heron.common.src.python.utils import log

from heron.proto import physical_plan_pb2, tuple_pb2, ckptmgr_pb2, common_pb2

from heron.instance.src.python.utils.misc import HeronCommunicator
from heron.instance.src.python.utils.misc import SerializerHelper
from heron.instance.src.python.utils.misc import PhysicalPlanHelper
from heron.instance.src.python.utils.metrics import GatewayMetrics, PyMetrics, MetricsCollector
from heron.instance.src.python.network import MetricsManagerClient, SingleThreadStmgrClient
from heron.instance.src.python.network import create_socket_options
from heron.instance.src.python.network import GatewayLooper
from heron.instance.src.python.basics import SpoutInstance, BoltInstance
import heron.instance.src.python.utils.system_constants as constants
from heron.instance.src.python.utils import system_config

Log = log.Log
AssignedInstance = collections.namedtuple('AssignedInstance', 'is_spout, protobuf, py_class')

def set_resource_limit(max_ram):
  resource.setrlimit(resource.RLIMIT_RSS, (max_ram, max_ram))

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
    self.serializer = None

    # my_instance is a AssignedInstance tuple
    self.my_instance = None
    self.is_instance_started = False
    self.is_stateful_started = False
    self.stateful_state = None

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

  def handle_initiate_stateful_checkpoint(self, ckptmsg):
    """Called when we get InitiateStatefulCheckpoint message
    :param ckptmsg: InitiateStatefulCheckpoint type
    """
    self.in_stream.offer(ckptmsg)
    if self.my_pplan_helper.is_topology_running():
      self.my_instance.py_class.process_incoming_tuples()

  def handle_start_stateful_processing(self, start_msg):
    """Called when we receive StartInstanceStatefulProcessing message
    :param start_msg: StartInstanceStatefulProcessing type
    """
    Log.info("Received start stateful processing for %s" % start_msg.checkpoint_id)
    self.is_stateful_started = True
    self.start_instance_if_possible()

  def handle_restore_instance_state(self, restore_msg):
    """Called when we receive RestoreInstanceStateRequest message
    :param restore_msg: RestoreInstanceStateRequest type
    """
    Log.info("Restoring instance state to checkpoint %s" % restore_msg.state.checkpoint_id)
    # Stop the instance
    if self.is_stateful_started:
      self.my_instance.py_class.stop()
      self.my_instance.py_class.clear_collector()
      self.is_stateful_started = False

    # Clear all buffers
    self.in_stream.clear()
    self.out_stream.clear()

    # Deser the state
    if self.stateful_state is not None:
      self.stateful_state.clear()
    if restore_msg.state.state is not None and restore_msg.state.state:
      try:
        self.stateful_state = self.serializer.deserialize(restore_msg.state.state)
      except Exception as e:
        raise RuntimeError("Could not serialize state during restore " + str(e))
    else:
      Log.info("The restore request does not have an actual state")
    if self.stateful_state is None:
      self.stateful_state = HashMapState()

    Log.info("Instance restore state deserialized")

    # Send the response back
    resp = ckptmgr_pb2.RestoreInstanceStateResponse()
    resp.status.status = common_pb2.StatusCode.Value("OK")
    resp.checkpoint_id = restore_msg.state.checkpoint_id
    self._stmgr_client.send_message(resp)

  def send_buffered_messages(self):
    """Send messages in out_stream to the Stream Manager"""
    while not self.out_stream.is_empty() and self._stmgr_client.is_registered:
      tuple_set = self.out_stream.poll()
      if isinstance(tuple_set, tuple_pb2.HeronTupleSet):
        tuple_set.src_task_id = self.my_pplan_helper.my_task_id
        self.gateway_metrics.update_sent_packet(tuple_set.ByteSize())
      self._stmgr_client.send_message(tuple_set)

  def _handle_state_change_msg(self, new_helper):
    """Called when state change is commanded by stream manager"""
    assert self.my_pplan_helper is not None
    assert self.my_instance is not None and self.my_instance.py_class is not None

    if self.my_pplan_helper.get_topology_state() != new_helper.get_topology_state():
      # handle state change
      if new_helper.is_topology_running():
        if not self.is_instance_started:
          self.start_instance_if_possible()
        self.my_instance.py_class.invoke_activate()
      elif new_helper.is_topology_paused():
        self.my_instance.py_class.invoke_deactivate()
      else:
        raise RuntimeError("Unexpected TopologyState update: %s" % new_helper.get_topology_state())
    else:
      Log.info("Topology state remains the same.")

    # update the pplan_helper
    self.my_pplan_helper = new_helper

  def handle_assignment_msg(self, pplan):
    """Called when new NewInstanceAssignmentMessage arrives

    Tells this instance to become either spout/bolt.

    :param pplan: PhysicalPlan proto
    """

    new_helper = PhysicalPlanHelper(pplan, self.instance.instance_id,
                                    self.topo_pex_file_abs_path)
    if self.my_pplan_helper is not None and \
      (self.my_pplan_helper.my_component_name != new_helper.my_component_name or
       self.my_pplan_helper.my_task_id != new_helper.my_task_id):
      raise RuntimeError("Our Assignment has changed. We will die to pick it.")

    new_helper.set_topology_context(self.metrics_collector)

    if self.my_pplan_helper is None:
      Log.info("Received a new Physical Plan")
      Log.info("Push the new pplan_helper to Heron Instance")
      self._handle_assignment_msg(new_helper)
    else:
      Log.info("Received a new Physical Plan with the same assignment -- State Change")
      Log.info("Old state: %s, new state: %s.",
               self.my_pplan_helper.get_topology_state(), new_helper.get_topology_state())
      self._handle_state_change_msg(new_helper)

  def _handle_assignment_msg(self, pplan_helper):
    self.my_pplan_helper = pplan_helper
    self.serializer = SerializerHelper.get_serializer(self.my_pplan_helper.context)

    if self.my_pplan_helper.is_spout:
      # Starting a spout
      my_spout = self.my_pplan_helper.get_my_spout()
      Log.info("Incarnating ourselves as spout: %s with task id %s",
               self.my_pplan_helper.my_component_name, str(self.my_pplan_helper.my_task_id))

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
      my_bolt = self.my_pplan_helper.get_my_bolt()
      Log.info("Incarnating ourselves as bolt: %s with task id %s",
               self.my_pplan_helper.my_component_name, str(self.my_pplan_helper.my_task_id))

      self.in_stream. \
        register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY])
      self.out_stream. \
        register_capacity(self.sys_config[constants.INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY])

      py_bolt_instance = BoltInstance(self.my_pplan_helper, self.in_stream, self.out_stream,
                                      self.looper)
      self.my_instance = AssignedInstance(is_spout=False,
                                          protobuf=my_bolt,
                                          py_class=py_bolt_instance)

    if self.my_pplan_helper.is_topology_running():
      try:
        self.start_instance_if_possible()
      except Exception as e:
        Log.error("Error with starting bolt/spout instance: " + str(e))
        Log.error(traceback.format_exc())
    else:
      Log.info("The instance is deployed in deactivated state")

  def start_instance_if_possible(self):
    if self.my_pplan_helper is None:
      return
    if not self.my_pplan_helper.is_topology_running():
      return
    context = self.my_pplan_helper.context
    mode = context.get_cluster_config().get(api_constants.TOPOLOGY_RELIABILITY_MODE,
                                            api_constants.TopologyReliabilityMode.ATMOST_ONCE)
    is_stateful = bool(mode == api_constants.TopologyReliabilityMode.EFFECTIVELY_ONCE)
    if is_stateful and not self.is_stateful_started:
      return
    try:
      Log.info("Starting bolt/spout instance now...")
      self.my_instance.py_class.start(self.stateful_state)
      self.is_instance_started = True
      Log.info("Started instance successfully.")
    except Exception as e:
      Log.error(traceback.format_exc())
      Log.error("Error when starting bolt/spout, bailing out...: %s", str(e))
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
  if len(sys.argv) != 14:
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
  override_config = yaml_config_reader(sys.argv[11])
  topology_pex_file_path = sys.argv[12]
  max_ram = int(sys.argv[13])

  system_config.set_sys_config(sys_config, override_config)

  # get combined configuration
  sys_config = system_config.get_sys_config()

  # set resource limits
  set_resource_limit(max_ram)

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
  Log.debug("Override config: " + str(override_config))
  Log.debug("Maximum Ram: " + str(max_ram))

  heron_instance = SingleThreadHeronInstance(topology_name, topology_id, instance, stmgr_port,
                                             metrics_port, topology_pex_file_path)
  heron_instance.start()

if __name__ == '__main__':
  main()
