import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class LogicalPlanHandler(BaseHandler):
  """
  URL - /topologies/logicalplan
  Parameters:
   - cluster (required)
   - environ (required)
   - topology (required) name of the requested topology

  The response JSON is a dictionary with all the
  information of logical plan of the topology.
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
      logical_plan = topologyInfo["logical_plan"]
      self.write_success_response(logical_plan)
    except Exception as e:
      self.write_error_response(e)

