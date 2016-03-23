import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.log import Log as LOG

from heron.proto import common_pb2
from heron.proto import tmaster_pb2

class MetricsHandler(BaseHandler):
  """
  URL - /topologies/metrics
  Parameters:
   - cluster (required)
   - environ (required)
   - topology (required) name of the requested topology
   - component (required)
   - metricname (required, repeated)
   - interval (optional)
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
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      component = self.get_argument_component()
      metric_names = self.get_required_arguments_metricnames()

      topology = self.tracker.getTopologyByClusterEnvironAndName(cluster, environ, topology_name)

      interval = int(self.get_argument(constants.PARAM_INTERVAL, default=-1))
      instances = self.get_arguments(constants.PARAM_INSTANCE)

      metrics = yield tornado.gen.Task(self.getComponentMetrics,
        topology.tmaster, component, metric_names, instances, interval)

      self.write_success_response(metrics)
    except Exception as e:
      self.write_error_response(e)

  @tornado.gen.coroutine
  def getComponentMetrics(self,
                          tmaster,
                          componentName,
                          metricNames,
                          instances,
                          interval,
                          callback=None):
    """
    Get the specified metrics for the given component name of this topology.
    Returns the following dict on success:
    {
      "metrics": {
        <metricname>: {
          <instance>: <numeric value>,
          <instance>: <numeric value>,
          ...
        }, ...
      },
      "interval": <numeric value>,
      "component": "..."
    }

    Raises exception on failure.
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
    metricRequest.interval = interval

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
    ret["interval"] = metricResponse.interval
    ret["component"] = componentName
    ret["metrics"] = {}
    for metric in metricResponse.metric:
      instance = metric.instance_id
      for im in metric.metric:
        metricname = im.name
        value = im.value
        if metricname not in ret["metrics"]:
          ret["metrics"][metricname] = {}
        ret["metrics"][metricname][instance] = value

    if not ret["metrics"]:
      raise Exception("No metrics found")

    raise tornado.gen.Return(ret)

