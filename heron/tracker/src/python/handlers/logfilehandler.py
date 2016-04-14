import json
import os
import tornado.gen
import traceback

from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python import utils

class LogfileDataHandler(BaseHandler):
  """
  URL - /topologies/logfiledata?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&instance=<instance>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - instance - Instance Id
   - offset - From which to read the file
   - length - How much data to read

  Get the data from the logfile for the given topology
  and instance. The data being read is based on offset and length.
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
      offset = self.get_argument_offset()
      length = self.get_argument_length()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, environ)

      stmgr_id = topology_info["physical_plan"]["instances"][instance]["stmgrId"]
      stmgr = topology_info["physical_plan"]["stmgrs"][stmgr_id]
      host = stmgr["host"]
      shell_port = stmgr["shell_port"]
      logfile_data_url = utils.make_shell_logfile_data_url(host, shell_port, instance, offset, length)

      http_client = tornado.httpclient.AsyncHTTPClient()
      response = yield http_client.fetch(logfile_data_url)
      self.write_success_response(json.loads(response.body))
      self.finish()
    except Exception as e:
      traceback.print_exc()
      self.write_error_response(e)
