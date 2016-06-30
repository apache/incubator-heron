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
from heron.common.src.python.color import Log
from instance.pplan_helper import PhysicalPlanHelper


class Slave(threading.Thread):
  def __init__(self, in_stream, out_stream, control_stream):
    super(Slave, self).__init__()
    self._in_stream = in_stream
    self._out_stream = out_stream
    self._control_stream = control_stream
    self._pplan_helper = None

  def run(self):
    try:
      while not self._control_stream.empty():
        msg = self._control_stream.poll()
        self.handle_control_message(msg)
    except Queue.Empty:
      pass

  def handle_control_message(self, ctrl_msg):
    if isinstance(ctrl_msg, PhysicalPlanHelper):
      # handle new physical plan
      if self._pplan_helper is None:
        self.handle_new_assignment(ctrl_msg)
      else:
        # TODO: Implement State change
        pass

      self._pplan_helper = ctrl_msg
    else:
      Log.error("Unrecognized control message type")

  def handle_new_assignment(self, ctrl_msg):
    pass
