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
import logging
import sys
import traceback

import signal
from heron.common.src.python.log import Log, init_logger
from heron.common.src.python.basics.gateway_looper import GatewayLooper
from heron.proto import physical_plan_pb2, stmgr_pb2
from heron.instance.src.python.single_thread_instance.single_thread_stmgr_client import SingleThreadStmgrClient
from heron.instance.src.python.misc.communicator import HeronCommunicator
import heron.common.src.python.pex_loader as pex_loader

class SingleThreadHeronInstance(object):
  def __init__(self, topology_name, topology_id, instance,
               stream_port, metrics_port, topo_pex_file_path):

    self.topology_name = topology_name
    self.topology_id = topology_id
    self.instance = instance
    self.stream_port = stream_port
    self.metrics_port = metrics_port
    self.topo_pex_file_path = topo_pex_file_path

    self.in_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)
    self.out_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)

    self._stmgr_client = SingleThreadStmgrClient(self, 'localhost', stream_port, topology_name,
                                                   topology_id, instance)
    self.my_pplan_helper = None

    # my_instance is a tuple containing (is_spout, TopologyAPI.{Spout|Bolt}, loaded python instance)
    self.my_instance = None
    self.looper = GatewayLooper()

    # Debugging purposes
    def go_trace(sig, stack):
      with open("/tmp/trace.log", "w") as f:
        traceback.print_stack(stack, file=f)
    signal.signal(signal.SIGUSR1, go_trace)

  def start(self):
    self._stmgr_client.start_connect()
    self.looper.prepare_map()
    self.looper.loop()

  def handle_new_tuple_set(self, tuple_msg_set):
    """Called when new TupleMessage arrives

    :param tuple_msg_set: HeronTupleSet type
    """
    if self.my_pplan_helper is None or \
        not self.my_pplan_helper.is_topology_running() or \
            self.my_instance is None:
      Log.error("Got tuple set when no instance assigned yet")
    else:
      # First add message to the in_stream
      self.in_stream.offer(tuple_msg_set)
      # Call run_in_single_thread() method
      self.my_instance[2].run_in_single_thread()
      # Send all messages
      self.send_buffered_messages()

  def send_buffered_messages(self):
    """Send messages in out_stream to the Stream Manager"""
    Log.debug("send_buffered_messages() called")
    while not self.out_stream.is_empty():
      tuple_set = self.out_stream.poll()
      msg = stmgr_pb2.TupleMessage()
      msg.set.CopyFrom(tuple_set)
      self._stmgr_client.send_message(msg)

  def handle_assignment_msg(self, pplan_helper):
    """Called when new NewInstanceAssignmentMessage arrives

    Tells this Instance to become either spout/bolt.
    Should be mostly equivalent to _handle_new_assignment() in slave.py

    :param pplan_helper: PhysicalPlanHelper class to become
    """
    Log.info("Incarnating ourselves as " + pplan_helper.my_component_name +
             " with task id " + str(pplan_helper.my_task_id))

    # TODO: bind the metrics collector with topology context

    self.my_pplan_helper = pplan_helper

    # TODO: handle STATE CHANGE

    # TODO (MOST IMPORTANT): handle importing pex file and load the class
    # -> need to load the class from python_class_name in Component
    try:
      if pplan_helper.is_spout:
        # Starting a spout
        my_spout = pplan_helper.get_my_spout()
        py_spout_instance = self.load_py_instance(True, my_spout.comp.python_class_name)
        self.my_instance = (True, my_spout, py_spout_instance)
      else:
        # Starting a bolt
        my_bolt = pplan_helper.get_my_bolt()
        py_bolt_instance = self.load_py_instance(False, my_bolt.comp.python_class_name)
        self.my_instance = (False, my_bolt, py_bolt_instance)

      if pplan_helper.is_topology_running():
        self.start_instance()
      else:
        Log.info("The instance is deployed in deactivated state")
    except Exception as e:
      Log.error("Error with loading bolt/spout instance from pex file")
      Log.error(traceback.format_exc())

  def load_py_instance(self, is_spout, python_class_name):
    # TODO : preliminary loading
    pex_loader.load_pex(self.topo_pex_file_path)
    if is_spout:
      spout_class = pex_loader.import_and_get_class(self.topo_pex_file_path, python_class_name)
      my_spout = spout_class(self.my_pplan_helper, self.in_stream, self.out_stream)
      return my_spout
    else:
      bolt_class = pex_loader.import_and_get_class(self.topo_pex_file_path, python_class_name)
      my_bolt = bolt_class(self.my_pplan_helper, self.in_stream, self.out_stream)
      return my_bolt

  def start_instance(self):
    Log.info("Start bolt/spout instance now...")
    self.my_instance[2].start()
    if self.my_instance[0]:
      # It's spout --> add task
      Log.info("Add spout task")
      def spout_task():
        self.my_instance[2].run_in_single_thread()
        self.send_buffered_messages()

      self.looper.add_wakeup_task(spout_task)

def print_usage():
  print("Usage: ./single_thread_heron_instance <topology_name> <topology_id> "
        "<instance_id> <component_name> <task_id> "
        "<component_index> <stmgr_id> <stmgr_port> <metricsmgr_port> "
        "<heron_internals_config_filename> <topology_pex_file_path>")

def main():
  if len(sys.argv) != 12:
    print_usage()
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
  system_config = sys.argv[10]
  topology_pex_file_path = sys.argv[11]

  # TODO: Add the SystemConfig into SingletonRegistory

  # create the protobuf instance
  instance_info = physical_plan_pb2.InstanceInfo()
  instance_info.task_id = int(task_id)
  instance_info.component_index = int(component_index)
  instance_info.component_name = component_name

  instance = physical_plan_pb2.Instance()
  instance.instance_id = instance_id
  instance.stmgr_id = stmgr_id
  instance.info.MergeFrom(instance_info)


  # TODO: improve later
  log_file = "/tmp/" + instance_id + ".log"
  init_logger(level=logging.INFO, logfile=log_file)

  Log.info("\nStarting instance: " + instance_id + " for topology: " + topology_name +
           " and topologyId: " + topology_id + " for component: " + component_name +
           " with taskId: " + task_id + " and componentIndex: " + component_index +
           " and stmgrId: " + stmgr_id + " and stmgrPort: " + stmgr_port +
           " and metricsManagerPort: " + metrics_port +
           "\n **Topology Pex file located at: " + topology_pex_file_path)

  heron_instance = SingleThreadHeronInstance(topology_name, topology_id, instance,
                                             stmgr_port, metrics_port, topology_pex_file_path)
  heron_instance.start()

if __name__ == '__main__':
  main()

