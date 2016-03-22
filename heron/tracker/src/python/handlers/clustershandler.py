from tornado.options import options

from heron.tracker.src.python.handlers import BaseHandler

class ClustersHandler(BaseHandler):
  """
  URL - /clusters

  The response JSON is a list of all the clusters
  that are tracked by the tracker.
  """
  def get(self):
    cluster = options.clusters
    ret = dict(
      clusters=[cluster]
    )
    self.write_success_response(ret)