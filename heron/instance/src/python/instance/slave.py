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
import Queue
import threading

# TODO: implement wait/notify mechanism
from heron.common.src.python.log import Log
from misc.pplan_helper import PhysicalPlanHelper


class Slave(threading.Thread):
  def __init__(self, in_stream, out_stream, control_stream):
    super(Slave, self).__init__()
    self._in_stream = in_stream
    self._out_stream = out_stream
    self._control_stream = control_stream
    self._pplan_helper = None
    self.my_instance = None

  def run(self):
    while True:
      try:
        while not self._control_stream.is_empty():
          msg = self._control_stream.poll()
          self._handle_control_message(msg)
      except Queue.Empty:
        pass

      try:
        while not self._in_stream.is_empty():
          pass
      except Queue.Empty:
        pass

  def _handle_control_message(self, ctrl_msg):
    if isinstance(ctrl_msg, PhysicalPlanHelper):
      # handle new physical plan
      if self._pplan_helper is None:
        self._handle_new_assignment(ctrl_msg)
      else:
        # TODO: Implement State change
        pass

      self._pplan_helper = ctrl_msg
    else:
      Log.error("Unrecognized control message type")

  def _handle_new_assignment(self, pplan_helper):
    Log.info("Incarnating ourselves as " + pplan_helper.my_component_name +
             " with task id " + pplan_helper.my_task_id)

    # TODO: bind the metrics collector with topology context

    # TODO (MOST IMPORTANT): handle importing pex file and load the class
    # -> need to load the class from python_class_name in Component
    if pplan_helper.is_spout:
      # Starting a spout
      self.my_instance = pplan_helper.get_my_spout()
    else:
      # Starting a bolt
      self.my_instance = pplan_helper.get_my_bolt()

    if pplan_helper.is_topology_running():
      self._start_instance()
    else:
      Log.info("The instance is deployed in deactivated state")

  def _start_instance(self):
    pass

