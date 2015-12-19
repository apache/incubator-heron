import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class TopologyHandler(BaseHandler):
  """
  url - /topologies/info
  Parameters:
   - dc (required)
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
      dc = self.get_argument_dc()
      environ = self.get_argument_environ()
      topName = self.get_argument_topology()
      topologyInfo = self.tracker.getTopologyInfo(topName, dc, environ)
      self.write_success_response(topologyInfo)
    except Exception as e:
      self.write_error_response(e)

