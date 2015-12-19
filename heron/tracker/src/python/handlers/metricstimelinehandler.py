import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.log import Log as LOG

from heron.proto import common_pb2
from heron.proto import tmaster_pb2

class MetricsTimelineHandler(BaseHandler):
  """
  URL - /topologies/metricstimeline
  Parameters:
   - dc (required)
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

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      dc = self.get_argument_dc()
      environ = self.get_argument_environ()
      topName = self.get_argument_topology()
      component = self.get_argument_component()
      metricNames = self.get_required_arguments_metricnames()
      startTime = self.get_argument_starttime()
      endTime = self.get_argument_endtime()
      self.validateInterval(startTime, endTime)
      instances = self.get_arguments(constants.PARAM_INSTANCE)

      topology = self.tracker.getTopologyByDcEnvironAndName(dc, environ, topName)
      metrics = yield tornado.gen.Task(self.getMetricsTimeline,
                                       topology.tmaster, component, metricNames,
                                       instances, int(startTime), int(endTime))
      self.write_success_response(metrics)
    except Exception as e:
      self.write_error_response(e)

  @tornado.gen.coroutine
  def getMetricsTimeline(self,
                         tmaster,
                         componentName,
                         metricNames,
                         instances,
                         startTime,
                         endTime,
                         callback=None):
    """
    Get the specified metrics for the given component name of this topology.
    Returns the following dict on success:
    {
      "timeline": {
        <metricname>: {
          <instance>: {
            <start_time> : <numeric value>,
            <start_time> : <numeric value>,
            ...
          }
          ...
        }, ...
      },
      "starttime": <numeric value>,
      "endtime": <numeric value>,
      "component": "..."
    }

    Returns the following dict on failure:
    {
      "message": "..."
    }
    """
    if not tmaster or not tmaster.host or not tmaster.stats_port:
      raise Exception("No Tmaster found")

    host = tmaster.host
    port = tmaster.stats_port

    metricRequest = tmaster_pb2.MetricRequest()
    metricRequest.component_name = componentName
    if len(instances) > 0:
      for instance in instances:
        metricRequest.instance_id.append(instance)
    for metricName in metricNames:
      metricRequest.metric.append(metricName)
    metricRequest.explicit_interval.start = startTime
    metricRequest.explicit_interval.end = endTime
    metricRequest.minutely = True

    # Serialize the metricRequest to send as a payload
    # with the HTTP request.
    metricRequestString = metricRequest.SerializeToString()

    url = "http://{0}:{1}/stats".format(host, port)
    request = tornado.httpclient.HTTPRequest(url,
                                             body=metricRequestString,
                                             method='POST',
                                             request_timeout=5)

    LOG.debug("Making HTTP call to fetch metrics")
    LOG.debug("url: " + url)
    try:
      client = tornado.httpclient.AsyncHTTPClient()
      result = yield client.fetch(request)
      LOG.debug("HTTP call complete.")
    except tornado.httpclient.HTTPError as e:
      raise Exception(str(e))


    # Check the response code - error if it is in 400s or 500s
    responseCode = result.code
    print responseCode
    if responseCode >= 400:
      message = "Error in getting metrics from Tmaster, code: " + responseCode
      LOG.error(message)
      raise Exception(message)

    # Parse the response from tmaster.
    metricResponse = tmaster_pb2.MetricResponse()
    metricResponse.ParseFromString(result.body)

    if metricResponse.status.status == common_pb2.NOTOK:
      if metricResponse.status.HasField("message"):
        raise Exception(metricResponse.status.message)

    # Form the response.
    ret = {}
    ret["starttime"] = startTime
    ret["endtime"] = endTime
    ret["component"] = componentName
    ret["timeline"] = {}
    for metric in metricResponse.metric:
      instance = metric.instance_id
      for im in metric.metric:
        metricname = im.name
        if metricname not in ret["timeline"]:
          ret["timeline"][metricname] = {}
        if instance not in ret["timeline"][metricname]:
          ret["timeline"][metricname][instance] = {}
        for interval_value in im.interval_values:
          ret["timeline"][metricname][instance][interval_value.interval.start] = interval_value.value

    if not ret["timeline"]:
      raise Exception("No metrics found")

    raise tornado.gen.Return(ret)

