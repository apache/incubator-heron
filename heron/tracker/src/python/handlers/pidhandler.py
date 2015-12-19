import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python import utils
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.log import Log as LOG

@tornado.gen.coroutine
def getInstancePid(topologyInfo, instance_id):
  """
  This method is used by other modules, and so it
  is not a part of the class.
  Fetches Instance pid from heron-shell.
  """
  try:
    http_client = tornado.httpclient.AsyncHTTPClient()
    endpoint = utils.make_shell_endpoint(topologyInfo, instance_id)
    url = "%s/pid/%s" % (endpoint, instance_id)
    LOG.debug("HTTP call for url: %s" % url)
    response = yield http_client.fetch(url)
    raise tornado.gen.Return(response.body)
  except tornado.httpclient.HTTPError as e:
    raise Exception(str(e))

class PidHandler(BaseHandler):
  """
  URL - /topologies/jmap?dc=<dc>&topology=<topology> \
        &environ=<environment>&instance=<instance>
  Parameters:
   - dc - Name of dc.
   - environ - Running environment.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - instance - Instance Id

  If successfule returns the pid of instance. May include training
  spaces and/or linefeed before/after.
  The response JSON is a dict with following format:
  {
     'command': Full command executed at server.
     'stdout': Text on stdout of executing the command.
     'stderr': <optional> Text on stderr.
  }
  """

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      dc = self.get_argument_dc()
      environ = self.get_argument_environ()
      topology = self.get_argument_topology()
      instance = self.get_argument_instance()
      topologyInfo = self.tracker.getTopologyInfo(topology, dc, environ)
      result = yield getInstancePid(topologyInfo, instance)
      self.write_success_response(result)
    except Exception as e:
      self.write_error_response(e)

