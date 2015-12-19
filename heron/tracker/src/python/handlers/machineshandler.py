import tornado.gen

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class MachinesHandler(BaseHandler):
  """
  URL - /machines
  Parameters:
   - dc (optional)
   - environ (optional)
   - topology (optional, repeated
               both 'dc' and 'environ' are required
               if topology is present)

  The response JSON is a dict with following format:
  {
    <dc1>: {
      <environ1>: {
        <top1>: [machine1, machine2,..],
        <top2>: [...],
        ...
      },
      <environ2>: {...},
      ...
    },
    <dc2>: {...}
  }
  """

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    dcs = self.get_arguments(constants.PARAM_DC)
    environs = self.get_arguments(constants.PARAM_ENVIRON)
    topNames = self.get_arguments(constants.PARAM_TOPOLOGY)

    ret = {}

    if len(topNames) > 1:
      if not dcs:
        message = "Missing argument" + constants.PARAM_DC
        self.write_error_response(message)
        return

      if not environs:
        message = "Missing argument" + constants.PARAM_ENVIRON
        self.write_error_response(message)
        return

    ret = {}
    topologies = self.tracker.topologies
    for topology in topologies:
      dc = topology.dc
      environ = topology.environ
      topName = topology.name
      if not dc or not environ:
        continue

      # This DC is not asked for.
      if dcs and dc not in dcs:
        continue

      # This environ is not asked for.
      if environs and environ not in environs:
        continue

      if topNames and topName not in topNames:
        continue

      if dc not in ret:
        ret[dc] = {}
      if environ not in ret[dc]:
        ret[dc][environ] = {}
      ret[dc][environ][topName] = topology.get_machines()

    self.write_success_response(ret)

