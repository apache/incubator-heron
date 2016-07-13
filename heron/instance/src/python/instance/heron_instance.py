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
import sys

from heron.common.src.python.log import Log
from heron.proto import physical_plan_pb2

from misc.communicator import HeronCommunicator
from .gateway import Gateway

# TODO (Very important): add topology_pex_file_path argument
class HeronInstance(object):
  def __init__(self, topology_name, topology_id, instance, stream_port, metrics_port):
    self.topology_name = topology_name
    self.topology_id = topology_id
    self.instance = instance
    self.stream_port = stream_port
    self.metrics_port = metrics_port

    # TODO: change from None to actual provider/consumer
    self._in_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)
    self._out_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)
    self._control_stream = HeronCommunicator(producer_cb=None, consumer_cb=None)

    self._gateway_thread = Gateway(topology_name, topology_id, instance, stream_port, metrics_port,
                                   self._in_stream, self._out_stream, self._control_stream)
    self._slave_thread = None

  def start(self):
    pass


def print_usage():
  print("Usage: ./heron-instance <topology_name> <topology_id> "
        "<instance_id> <component_name> <task_id> "
        "<component_index> <stmgr_id> <stmgr_port> <metricsmgr_port> "
        "<heron_internals_config_filename>")


def main():
  if len(sys.argv) != 10:
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

  # TODO: Add the SystemConfig into SingletonRegistory

  # create the protobuf instance
  instance_info = physical_plan_pb2.InstanceInfo()
  instance_info.task_id = task_id
  instance_info.component_index = component_index
  instance_info.component_name = component_name

  instance = physical_plan_pb2.Instance()
  instance.instance_id = instance_id
  instance.stmgr_id = stmgr_id
  instance.info.MergeFrom(instance_info)


  Log.info("\nStarting instance " + instance_id + " for topology " + topology_name +
           " and topologyId " + topology_id + " for component " + component_name +
           " with taskId " + task_id + " and componentIndex " + component_index +
           " and stmgrId " + stmgr_id + " and stmgrPort " + stmgr_port +
           " and metricsManagerPort " + metrics_port)

  heron_instance = HeronInstance(topology_name, topology_id, instance, stmgr_port, metrics_port)
  heron_instance.start()

if __name__ == '__main__':
  main()

