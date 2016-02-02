import json
import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python import utils
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.handlers.pidhandler import getInstancePid
from heron.tracker.src.python.log import Log as LOG

class JstackHandler(BaseHandler):
  """
  URL - /topologies/jstack?dc=<dc>&topology=<topology> \
        &environ=<environment>&instance=<instance>
  Parameters:
   - dc - Name of dc.
   - environ - Running environment.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - instance - Instance Id

  Resturns a thread dump of the instance on stdout.
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
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      instance = self.get_argument_instance()
      topology_info = self.tracker.getTopologyInfo(topology, cluster, environ)
      ret = yield self.getInstanceJstack(topology_info, instance)
      self.write_success_response(ret)
    except Exception as e:
      self.write_error_response(e)

  @tornado.gen.coroutine
  def getInstanceJstack(self, topology_info, instance_id):
    """
    Fetches Instance jstack from heron-shell.
    """
    pid_response = yield getInstancePid(topology_info, instance_id)
    try:
      http_client = tornado.httpclient.AsyncHTTPClient()
      component_id = instance_id.split('_')[1] # Format: container_<id>_<instance_id>
      pid_json = json.loads(pid_response)
      pid = pid_json['stdout'].strip()
      if pid == '':
        raise Exception('Failed to get pid')
      endpoint = utils.make_shell_endpoint(topology_info, instance_id)
      url = "%s/jstack/%s" % (endpoint, pid)
      response = yield http_client.fetch(url)
      LOG.debug("HTTP call for url: %s" % url)
      raise tornado.gen.Return(response.body)
    except tornado.httpclient.HTTPError as e:
      raise Exception(str(e))

