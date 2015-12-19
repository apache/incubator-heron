import tornado.gen

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class TopologiesHandler(BaseHandler):
  """
  URL - /topologies
  Parameters:
   - dc (optional)
   - environ (optional)

  The response JSON is a dict with following format:
  {
    <dc1>: {
      <environ1>: [
        top1,
        top2,
        ...
      ],
      <environ2>: [...],
      ...
    },
    <dc2>: {...}
  }
  """
  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    # Get all the values for parameter "dc".
    dcs = self.get_arguments(constants.PARAM_DC)
    # Get all the values for parameter "environ".
    environs = self.get_arguments(constants.PARAM_ENVIRON)

    ret = {}
    topologies = self.tracker.topologies
    for topology in topologies:
      dc = topology.dc
      environ = topology.environ
      if not dc or not environ:
        continue

      # This DC is not asked for.
      # Note that "if not dcs", then
      # we show for all the dcs.
      if dcs and dc not in dcs:
        continue

      # This environ is not asked for.
      # Note that "if not environs", then
      # we show for all the environs.
      if environs and environ not in environs:
        continue

      if dc not in ret:
        ret[dc] = {}
      if environ not in ret[dc]:
        ret[dc][environ] = []
      ret[dc][environ].append(topology.name)
    self.write_success_response(ret)

