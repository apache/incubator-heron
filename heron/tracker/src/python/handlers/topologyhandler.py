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

import tornado.gen
import tornado.web
import traceback

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class TopologyHandler(BaseHandler):
  """
  url - /topologies/info
  Parameters:
   - cluster (required)
   - environ (required)
   - topology (required) name of the requested topology
   - role (optional)

  the response json is a dictionary with all the
  information of the topology, including its
  logical and physical plan.
  """
  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      role = self.get_argument(constants.PARAM_ROLE, default=None)
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)
      self.write_success_response(topology_info)
    except Exception as e:
      traceback.print_exc()
      self.write_error_response(e)

