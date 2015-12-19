import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.query import Query

class MetricsQueryHandler(BaseHandler):
  """
  URL - /topologies/metricsquery
  Parameters:
   - dc (required)
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
      dc = self.get_argument_dc()
      environ = self.get_argument_environ()
      topName = self.get_argument_topology()
      topology = self.tracker.getTopologyByDcEnvironAndName(dc, environ, topName)

      startTime = self.get_argument_starttime()
      endTime = self.get_argument_endtime()
      self.validateInterval(startTime, endTime)

      query = self.get_argument_query()
      metrics = yield tornado.gen.Task(self.executeMetricsQuery,
                                       topology.tmaster, query, int(startTime), int(endTime))
      self.write_success_response(metrics)
    except Exception as e:
      self.write_error_response(e)

  @tornado.gen.coroutine
  def executeMetricsQuery(self,
                         tmaster,
                         queryString,
                         startTime,
                         endTime,
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
    metrics = yield query.execute_query(tmaster, queryString, startTime, endTime)

    # Parse the response
    ret = {}
    ret["starttime"] = startTime
    ret["endtime"] = endTime
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

