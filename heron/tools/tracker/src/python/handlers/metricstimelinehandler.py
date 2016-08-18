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
''' metricstimelinehandler.py '''
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python import metricstimeline
from heron.tools.tracker.src.python.handlers import BaseHandler

class MetricsTimelineHandler(BaseHandler):
  """
  URL - /topologies/metricstimeline
  Parameters:
   - cluster (required)
   - role - (optional) Role used to submit the topology.
   - environ (required)
   - topology (required) name of the requested topology
   - component (required)
   - metricname (required, repeated)
   - starttime (required)
   - endtime (required)
   - instance (optional, repeated)

  The response JSON is a map of all the requested
  (or if nothing is mentioned, all) components
  of the topology, to the metrics that are reported
  by that component.
  """

  # pylint: disable=attribute-defined-outside-init
  def initialize(self, tracker):
    """ initialize """
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    """ get method """
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      component = self.get_argument_component()
      metric_names = self.get_required_arguments_metricnames()
      start_time = self.get_argument_starttime()
      end_time = self.get_argument_endtime()
      self.validateInterval(start_time, end_time)
      instances = self.get_arguments(constants.PARAM_INSTANCE)

      topology = self.tracker.getTopologyByClusterRoleEnvironAndName(
          cluster, role, environ, topology_name)
      metrics = yield tornado.gen.Task(metricstimeline.getMetricsTimeline,
                                       topology.tmaster, component, metric_names,
                                       instances, int(start_time), int(end_time))
      self.write_success_response(metrics)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
