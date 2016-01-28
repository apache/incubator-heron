import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.log import Log as LOG

from heron.proto import common_pb2
from heron.proto import tmaster_pb2

class ExceptionSummaryHandler(BaseHandler):
  """
  URL - /topologies/exceptionsummary?dc=<dc>&topology=<topology> \
        &environ=<environment>&component=<component>
  Parameters:
   - dc - Name of dc.
   - environ - Running environment.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - component - Component name
   - instance - (optional, repeated)

  Returns summary of the exceptions for the component of the topology.
  Duplicated exceptions are combined together and shows the number of
  occurances, first occurance time and latest occurance time.
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
      topology = self.tracker.getTopologyByDcEnvironAndName(cluster, environ, topology_name)
      instances = self.get_arguments(constants.PARAM_INSTANCE)
      exceptions_summary = yield tornado.gen.Task(self.getComponentExceptionSummary,
                                                  topology.tmaster, component, instances)
      result = self.make_success_response(exceptions_summary)
      self.write_success_response(exceptions_summary)
    except Exception as e:
      self.write_error_response(e)

  @tornado.gen.coroutine
  def getComponentExceptionSummary(self, tmaster, component_name, instances=[], callback=None):
    """
    Get the summary of exceptions for component_name and list of instances.
    Empty instance list will fetch all exceptions.
    """
    if not tmaster or not tmaster.host or not tmaster.stats_port:
      return
    exception_request = tmaster_pb2.ExceptionLogRequest()
    exception_request.component_name = component_name
    if len(instances) > 0:
      exception_request.instances.extend(instances)
    request_str = exception_request.SerializeToString()
    port = str(tmaster.stats_port)
    host = tmaster.host
    url = "http://{0}:{1}/exceptionsummary".format(host, port)
    print "Creating request object."
    request = tornado.httpclient.HTTPRequest(url,
                                             body=request_str,
                                             method='POST',
                                             request_timeout=5)
    LOG.debug('Making HTTP call to fetch exceptionsummary url: %s' % url)
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
      message = "Error in getting exceptions from Tmaster, code: " + responseCode
      LOG.error(message)
      raise tornado.gen.Return({
        "message": message
      })

    # Parse the response from tmaster.
    exception_response = tmaster_pb2.ExceptionLogResponse()
    exception_response.ParseFromString(result.body)

    if exception_response.status.status == common_pb2.NOTOK:
      if exception_response.status.HasField("message"):
        raise tornado.gen.Return({
          "message": exception_response.status.message
        })

    # Send response
    ret = []
    for exception_log in exception_response.exceptions:
      ret.append({'class_name': exception_log.stacktrace,
                  'lasttime': exception_log.lasttime,
                  'firsttime': exception_log.firsttime,
                  'count': str(exception_log.count)})
    raise tornado.gen.Return(ret)

