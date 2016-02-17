import tornado.gen

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class MachinesHandler(BaseHandler):
  """
  URL - /machines
  Parameters:
   - cluster (optional)
   - environ (optional)
   - topology (optional, repeated
               both 'cluster' and 'environ' are required
               if topology is present)

  The response JSON is a dict with following format:
  {
    <cluster1>: {
      <environ1>: {
        <top1>: [machine1, machine2,..],
        <top2>: [...],
        ...
      },
      <environ2>: {...},
      ...
    },
    <cluster2>: {...}
  }
  """

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    clusters = self.get_arguments(constants.PARAM_CLUSTER)
    environs = self.get_arguments(constants.PARAM_ENVIRON)
    topology_names = self.get_arguments(constants.PARAM_TOPOLOGY)

    ret = {}

    if len(topology_names) > 1:
      if not dcs:
        message = "Missing argument" + constants.PARAM_CLUSTER
        self.write_error_response(message)
        return

      if not environs:
        message = "Missing argument" + constants.PARAM_ENVIRON
        self.write_error_response(message)
        return

    ret = {}
    topologies = self.tracker.topologies
    for topology in topologies:
      cluster = topology.cluster
      environ = topology.environ
      topology_name = topology.name
      if not cluster or not environ:
        continue

      # This cluster is not asked for.
      if clusters and cluster not in clusters:
        continue

      # This environ is not asked for.
      if environs and environ not in environs:
        continue

      if topology_names and topology_name not in topology_names:
        continue

      if cluster not in ret:
        ret[cluster] = {}
      if environ not in ret[cluster]:
        ret[cluster][environ] = {}
      ret[cluster][environ][topology_name] = topology.get_machines()

    self.write_success_response(ret)

