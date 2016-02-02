import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.query import Query

class MetricsQueryHandler(BaseHandler):
  """
  URL - /topologies/metricsquery
  Parameters:
   - cluster (required)
   - environ (required)
   - topology (required) name of the requested topology
   - starttime (required)
   - endtime (required)
   - query (required)

  The response JSON is a list of timelines
  asked in the query for this topology
  """

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      topology = self.tracker.getTopologyByDcEnvironAndName(cluster, environ, topology_name)

      start_time = self.get_argument_starttime()
      end_time = self.get_argument_endtime()
      self.validateInterval(start_time, end_time)

      query = self.get_argument_query()
      metrics = yield tornado.gen.Task(self.executeMetricsQuery,
                                       topology.tmaster, query, int(start_time), int(end_time))
      self.write_success_response(metrics)
    except Exception as e:
      self.write_error_response(e)

  @tornado.gen.coroutine
  def executeMetricsQuery(self,
                         tmaster,
                         queryString,
                         start_time,
                         end_time,
                         callback=None):
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

    if not metrics:
      raise Exception("No metrics found")

    for metric in metrics:
      tl = {
        "data": metric.timeline
      }
      if metric.instance:
        tl["instance"] = metric.instance
      ret["timeline"].append(tl)

    if not ret["timeline"]:
      raise Exception("No metrics found")

    raise tornado.gen.Return(ret)

