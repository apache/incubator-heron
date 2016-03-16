import os, sys
import argparse

import tornado.ioloop
import tornado.options
import tornado.web
import tornado.log
import tornado.template

from tornado.options import options, define

from heron.ui.src.python import handlers
from heron.ui.src.python import args
from heron.ui.src.python import log
from heron.ui.src.python.log import Log as LOG

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

def define_options(port, tracker_url):
  define("port", default=port)
  define("tracker_url", default=tracker_url)

def main(argv):
  log.configure(log.logging.DEBUG)
  tornado.log.enable_pretty_logging()

  # create the parser and parse the arguments
  (parser, ya_parser) = args.create_parsers()
  (clargs, remaining) = parser.parse_known_args()
  if remaining:
    yaargs = ya_parser.parse_args(args = remaining, namespace=clargs)
    parser.print_help()
    parser.exit()

  # log additional information
  namespace = vars(clargs)
  LOG.info("Running on port: %d", namespace['port'])
  LOG.info("Using tracker url: %s", namespace['tracker_url'])

  # pass the options to tornado and start the ui server
  define_options(namespace['port'], namespace['tracker_url'])
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(namespace['port'])
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main(sys.argv)
