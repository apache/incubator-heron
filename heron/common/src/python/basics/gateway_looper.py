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
import time

from event_looper import EventLooper
import asyncore

class GatewayLooper(EventLooper):
  def __init__(self):
    super(GatewayLooper, self).__init__()
    self.map = None
    self.register_timer_task_in_sec(self.exit_loop, 5)
    self.started = time.asctime()
    def at_exit():
      print self.started
      print time.asctime()
    self.add_exit_task(at_exit)

  def prepare_map(self):
    self.map = asyncore.socket_map

  def do_wait(self):
    next_timeout = self.get_next_timeout_interval()
    if next_timeout > 0:
      asyncore.poll(timeout=next_timeout, map=self.map)
    else:
      asyncore.poll(timeout=0.0, map=self.map)

  def wake_up(self):
    #TODO: Implement
    pass
