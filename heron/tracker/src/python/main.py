import os
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.escape import json_encode, utf8
from tornado.options import define, options

from heron.tracker.src.python import handlers
from heron.tracker.src.python import log
from heron.tracker.src.python.log import Log as LOG
from heron.tracker.src.python.tracker import Tracker

define("stateconf", default='zkstateconf', help="Yaml config file without extension for state locations")
define("port", default=8888, type=int, help="HTTP port to run the Tracker")

class Application(tornado.web.Application):
  def __init__(self):
    tracker = Tracker()
    self.tracker = tracker
    tracker.synch_topologies(options.stateconf)
    tornadoHandlers = [
      (r"/", handlers.MainHandler),
      (r"/topologies", handlers.TopologiesHandler, {"tracker":tracker}),
      (r"/topologies/states", handlers.StatesHandler, {"tracker":tracker}),
      (r"/topologies/info", handlers.TopologyHandler, {"tracker":tracker}),
      (r"/topologies/logicalplan", handlers.LogicalPlanHandler, {"tracker":tracker}),
      (r"/topologies/physicalplan", handlers.PhysicalPlanHandler, {"tracker":tracker}),
      (r"/topologies/executionstate", handlers.ExecutionStateHandler, {"tracker":tracker}),
      (r"/topologies/metrics", handlers.MetricsHandler, {"tracker":tracker}),
      (r"/topologies/metricstimeline", handlers.MetricsTimelineHandler, {"tracker":tracker}),
      (r"/topologies/metricsquery", handlers.MetricsQueryHandler, {"tracker":tracker}),
      (r"/topologies/exceptions", handlers.ExceptionHandler, {"tracker":tracker}),
      (r"/topologies/exceptionsummary", handlers.ExceptionSummaryHandler, {"tracker":tracker}),
      (r"/machines", handlers.MachinesHandler, {"tracker":tracker}),
      (r"/topologies/pid", handlers.PidHandler, {"tracker":tracker}),
      (r"/topologies/jstack", handlers.JstackHandler, {"tracker":tracker}),
      (r"/topologies/jmap", handlers.JmapHandler, {"tracker":tracker}),
      (r"/topologies/histo", handlers.MemoryHistogramHandler, {"tracker":tracker}),
      (r"(.*)", handlers.DefaultHandler),
    ]
    settings = dict(
      static_path = os.path.dirname(__file__),
      debug = True,
      serve_traceback = True
    )
    tornado.web.Application.__init__(self, tornadoHandlers, **settings)

def main():
  log.configure(log.logging.DEBUG)
  options.parse_command_line()
  port = options.port
  LOG.info("Running on port: " + str(port))
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

