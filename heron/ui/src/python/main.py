# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# !/usr/bin/env python2.7
''' main.py '''
import os
import sys
import socket

import tornado.ioloop
import tornado.options
import tornado.web
import tornado.log
import tornado.template

from tornado.options import define

from heron.ui.src.python import handlers
from heron.ui.src.python import args
from heron.ui.src.python import log
from heron.ui.src.python.log import Log as LOG


class Application(tornado.web.Application):
  ''' Application '''

  def __init__(self):
    # TODO: Change these to query string parameters, since
    # current format can lead to pattern matching issues.
    # pylint: disable=line-too-long,bad-whitespace
    callbacks = [
        (r"/", handlers.MainHandler),

        (r"/topologies", handlers.ListTopologiesHandler),
        (r"/topologies/filestats/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)", handlers.ContainerFileStatsHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/config", handlers.TopologyConfigHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptions", handlers.TopologyExceptionsPageHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)", handlers.TopologyPlanHandler),

        # topology metric apis
        (r"/topologies/metrics", handlers.api.MetricsHandler),
        (r"/topologies/metrics/timeline", handlers.api.MetricsTimelineHandler),

        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/file", handlers.ContainerFileHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/filedata", handlers.ContainerFileDataHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/filedownload", handlers.ContainerFileDownloadHandler),

        # Topology list and plan handlers
        (r"/topologies/list.json", handlers.api.ListTopologiesJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/logicalplan.json", handlers.api.TopologyLogicalPlanJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/physicalplan.json", handlers.api.TopologyPhysicalPlanJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/executionstate.json", handlers.api.TopologyExecutionStateJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/schedulerlocation.json",
         handlers.api.TopologySchedulerLocationJsonHandler),

        # Counter Handlers
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptions.json", handlers.api.TopologyExceptionsJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptionsummary.json",
         handlers.api.TopologyExceptionSummaryHandler),

        # Heron shell Handlers
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/pid", handlers.api.PidHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/jstack", handlers.api.JstackHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/jmap", handlers.api.JmapHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/histo", handlers.api.MemoryHistogramHandler),
    ]

    settings = dict(
        template_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../resources/templates"),
        static_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../resources/static"),
        gzip=True,
        debug=True,
        default_handler_class=handlers.NotFoundHandler,
    )
    tornado.web.Application.__init__(self, callbacks, **settings)


def define_options(port, tracker_url):
  '''
  :param port:
  :param tracker_url:
  :return:
  '''
  define("port", default=port)
  define("tracker_url", default=tracker_url)


# pylint: disable=unused-argument
def main(argv):
  '''
  :param argv:
  :return:
  '''
  log.configure(log.logging.DEBUG)
  tornado.log.enable_pretty_logging()

  # create the parser and parse the arguments
  (parser, child_parser) = args.create_parsers()
  (parsed_args, remaining) = parser.parse_known_args()
  if remaining:
    child_parser.parse_args(args=remaining, namespace=parsed_args)
    parser.print_help()
    parser.exit()

  # log additional information
  command_line_args = vars(parsed_args)
  address = socket.gethostbyname(socket.gethostname())
  LOG.info("Listening at http://%s:%d", address, command_line_args['port'])
  LOG.info("Using tracker url: %s", command_line_args['tracker_url'])

  # pass the options to tornado and start the ui server
  define_options(command_line_args['port'], command_line_args['tracker_url'])
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(command_line_args['port'])
  tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
  main(sys.argv)
