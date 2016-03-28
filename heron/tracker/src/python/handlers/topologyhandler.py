import tornado.gen
import tornado.web
import traceback

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class TopologyHandler(BaseHandler):
  """
  url - /topologies/info
  Parameters:
   - cluster (required)
   - environ (required)
   - topology (required) name of the requested topology

  the response json is a dictionary with all the
  information of the topology, including its
  logical and physical plan.
  """
  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, environ)
      self.write_success_response(topology_info)
    except Exception as e:
      traceback.print_exc()
      self.write_error_response(e)

