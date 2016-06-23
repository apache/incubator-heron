import json
import tornado.gen
import traceback

from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python import constants
from heron.tracker.src.python import utils


class ContainerFileDataHandler(BaseHandler):
  """
  URL - /topologies/containerfiledata?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&container=<container>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
   - role - (optional) Role used to submit the topology.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - container - Container number
   - path - Relative path to the file
   - offset - From which to read the file
   - length - How much data to read

  Get the data from the file for the given topology, container
  and path. The data being read is based on offset and length.
  """
  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      role = self.get_argument(constants.PARAM_ROLE, default=None)
      container = self.get_argument(constants.PARAM_CONTAINER)
      path = self.get_argument(constants.PARAM_PATH)
      offset = self.get_argument_offset()
      length = self.get_argument_length()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)

      stmgr_id = "stmgr-" + container
      stmgr = topology_info["physical_plan"]["stmgrs"][stmgr_id]
      host = stmgr["host"]
      shell_port = stmgr["shell_port"]
      file_data_url = "http://%s:%d/filedata/%s?offset=%s&length=%s" % (host, shell_port, path, offset, length)

      http_client = tornado.httpclient.AsyncHTTPClient()
      response = yield http_client.fetch(file_data_url)
      self.write_success_response(json.loads(response.body))
      self.finish()
    except Exception as e:
      traceback.print_exc()
      self.write_error_response(e)


class ContainerFileStatsHandler(BaseHandler):
  """
  URL - /topologies/containerfilestats?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&container=<container>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
<<<<<<< f57e472c35d9eacc611d348b151a7579987c72cc
   - role - (optional) Role used to submit the topology.
=======
   - role - Name of person who submits the topology (optional)
>>>>>>> Adapt to tracker API with new parameter.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - container - Container number
   - path - Relative path to the directory

  Get the dir stats for the given topology, container and path.
  """

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      role = self.get_argument(constants.PARAM_ROLE, default=None)
      container = self.get_argument(constants.PARAM_CONTAINER)
      path = self.get_argument(constants.PARAM_PATH, default=".")
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)

      stmgr_id = "stmgr-" + str(container)
      stmgr = topology_info["physical_plan"]["stmgrs"][stmgr_id]
      host = stmgr["host"]
      shell_port = stmgr["shell_port"]
      filestats_url = utils.make_shell_filestats_url(host, shell_port, path)

      http_client = tornado.httpclient.AsyncHTTPClient()
      response = yield http_client.fetch(filestats_url)
      self.write_success_response(json.loads(response.body))
      self.finish()
    except Exception as e:
      traceback.print_exc()
      self.write_error_response(e)
