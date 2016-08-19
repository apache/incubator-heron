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
''' metricsqueryhandler.py '''
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python.handlers import BaseHandler
from heron.tools.tracker.src.python.query import Query


class MetricsQueryHandler(BaseHandler):
  """
  URL - /topologies/metricsquery
  Parameters:
   - cluster (required)
   - role - (role) Role used to submit the topology.
   - environ (required)
   - topology (required) name of the requested topology
   - starttime (required)
   - endtime (required)
   - query (required)

  The response JSON is a list of timelines
  asked in the query for this topology
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
      topology = self.tracker.getTopologyByClusterRoleEnvironAndName(
          cluster, role, environ, topology_name)

      start_time = self.get_argument_starttime()
      end_time = self.get_argument_endtime()
      self.validateInterval(start_time, end_time)

      query = self.get_argument_query()
      metrics = yield tornado.gen.Task(self.executeMetricsQuery,
                                       topology.tmaster, query, int(start_time), int(end_time))
      self.write_success_response(metrics)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)

  # pylint: disable=unused-argument
  @tornado.gen.coroutine
  def executeMetricsQuery(self, tmaster, queryString, start_time, end_time, callback=None):
    """
    Get the specified metrics for the given query in this topology.
    Returns the following dict on success:
    {
      "timeline": [{
        "instance": <instance>,
        "data": {
          <start_time> : <numeric value>,
          <start_time> : <numeric value>,
          ...
        }
      }, {
        ...
      }, ...
      "starttime": <numeric value>,
      "endtime": <numeric value>,
    },

    Returns the following dict on failure:
    {
      "message": "..."
    }
    """

    query = Query(self.tracker)
    metrics = yield query.execute_query(tmaster, queryString, start_time, end_time)

    # Parse the response
    ret = {}
    ret["starttime"] = start_time
    ret["endtime"] = end_time
    ret["timeline"] = []

    for metric in metrics:
      tl = {
          "data": metric.timeline
      }
      if metric.instance:
        tl["instance"] = metric.instance
      ret["timeline"].append(tl)

    raise tornado.gen.Return(ret)
