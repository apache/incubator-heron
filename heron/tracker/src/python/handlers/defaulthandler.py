import tornado.gen

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class DefaultHandler(BaseHandler):
  """
  URL - anything that is not supported

  This is the default case in the regular expression
  matching for the URLs. If nothin matched before this,
  then this is the URL that is not supported by the API.

  Sends back a "failure" response to the client.
  """

  @tornado.gen.coroutine
  def get(self, url):
    self.write_error_response("URL not supported: " + url)

