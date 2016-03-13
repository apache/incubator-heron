import os
import sys

import tornado.ioloop
import tornado.options
import tornado.web
import tornado.log
import tornado.template

from tornado.options import options, define

from heron.ui.src.python import handlers

# default params
# port for the web to ui to listen on
DEFAULT_PORT = 8889
# url to connect for to heron tracker for metrics
DEFAULT_TRACKER_URL = "http://localhost"

class Application(tornado.web.Application):
  def __init__(self):
    callbacks = [
      (r"/",                                                                     handlers.MainHandler),

      (r"/topologies",                                                           handlers.ListTopologiesHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/config",                         handlers.TopologyConfigHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptions",   handlers.TopologyExceptionsPageHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)",                                handlers.TopologyPlanHandler),

      # topology metric apis
      (r"/topologies/metrics",                                                   handlers.api.MetricsHandler),
      (r"/topologies/metrics/timeline",                                          handlers.api.MetricsTimelineHandler),

      # Topology list and plan handlers
      (r"/topologies/list.json",                                                 handlers.api.ListTopologiesJsonHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/logicalplan.json",               handlers.api.TopologyLogicalPlanJsonHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/physicalplan.json",              handlers.api.TopologyPhysicalPlanJsonHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/executionstate.json",            handlers.api.TopologyExecutionStateJsonHandler),

      # Counter Handlers
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptions.json",       handlers.api.TopologyExceptionsJsonHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptionsummary.json", handlers.api.TopologyExceptionSummaryHandler),

      # Heron shell Handlers
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/pid",                   handlers.api.PidHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/jstack",                handlers.api.JstackHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/jmap",                  handlers.api.JmapHandler),
      (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/histo",                 handlers.api.MemoryHistogramHandler),
    ]

    settings = dict(
      template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../resources/templates"),
      static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../resources/static"),
      gzip=True,
      debug=True,
      default_handler_class = handlers.NotFoundHandler,
    )
    tornado.web.Application.__init__(self, callbacks, **settings)

def define_options():
  define("port", default=DEFAULT_PORT)
  define("tracker_url", default=DEFAULT_TRACKER_URL)

def main(argv):
  define_options()
  tornado.options.parse_command_line()

  # enable logging for tornado
  tornado.log.enable_pretty_logging()

  port = options.port
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main(sys.argv)
