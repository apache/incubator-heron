from heron.tracker.src.python.handlers import BaseHandler

class MainHandler(BaseHandler):
  """
  URL - /

  The response JSON is a list of all the API URLs
  that are supported by tracker.
  """
  def get(self):
    self.redirect("/topologies")

