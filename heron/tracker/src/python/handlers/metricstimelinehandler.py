import tornado.gen
import tornado.web

from heron.tracker.src.python import constants
from heron.tracker.src.python import metricstimeline
from heron.tracker.src.python.handlers import BaseHandler
from heron.tracker.src.python.log import Log as LOG

from heron.proto import common_pb2
from heron.proto import tmaster_pb2

class MetricsTimelineHandler(BaseHandler):
  """
  URL - /topologies/metricstimeline
  Parameters:
   - cluster (required)
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
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      component = self.get_argument_component()
      metric_names = self.get_required_arguments_metricnames()
      start_time = self.get_argument_starttime()
      end_time = self.get_argument_endtime()
      self.validateInterval(start_time, end_time)
      instances = self.get_arguments(constants.PARAM_INSTANCE)

      topology = self.tracker.getTopologyByClusterEnvironAndName(cluster, environ, topology_name)
      metrics = yield tornado.gen.Task(metricstimeline.getMetricsTimeline,
                                       topology.tmaster, component, metric_names,
                                       instances, int(start_time), int(end_time))
      self.write_success_response(metrics)
    except Exception as e:
      self.write_error_response(e)
