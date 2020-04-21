#!/usr/bin/env python2.7
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

''' main.py '''
from __future__ import print_function
import logging
import os
import signal

import sys
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.log
import tornado.template
from tornado.httpclient import AsyncHTTPClient
from tornado.options import define
from tornado.web import url

import heron.common.src.python.utils.log as log
import heron.tools.common.src.python.utils.config as common_config
from heron.tools.ui.src.python import handlers
from heron.tools.ui.src.python import args

Log = log.Log

class Application(tornado.web.Application):
  ''' Application '''

  def __init__(self, base_url):

    # pylint: disable=fixme
    # TODO: hacky solution
    # sys.path[0] should be the path to the extracted files for heron-ui, as it is added
    # when bootstrapping the pex file
    static_prefix = '/static/'
    if base_url != "":
      static_prefix = os.path.join(base_url, 'static/')

    AsyncHTTPClient.configure(None, defaults=dict(request_timeout=120.0))
    Log.info("Using base url: %s", base_url)
    settings = dict(
        template_path=os.path.join(sys.path[0], "heron/tools/ui/resources/templates"),
        static_path=os.path.join(sys.path[0], "heron/tools/ui/resources/static"),
        static_url_prefix=static_prefix,
        gzip=True,
        debug=True,
        default_handler_class=handlers.NotFoundHandler,
    )
    Log.info(os.path.join(base_url, 'static/'))

    # Change these to query string parameters, since
    # current format can lead to pattern matching issues.
    callbacks = [
        (r"/", handlers.MainHandler),

        url(r"/topologies", handlers.ListTopologiesHandler, dict(baseUrl=base_url),
            name='topologies'),
        url(r"/topologies/filestats/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)",
            handlers.ContainerFileStatsHandler, dict(baseUrl=base_url)),
        url(r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/config",
            handlers.TopologyConfigHandler, dict(baseUrl=base_url)),
        url(r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptions",
            handlers.TopologyExceptionsPageHandler, dict(baseUrl=base_url)),
        url(r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)",
            handlers.TopologyPlanHandler, dict(baseUrl=base_url)),

        # topology metric apis
        (r"/topologies/metrics",
         handlers.api.MetricsHandler),
        (r"/topologies/metrics/timeline",
         handlers.api.MetricsTimelineHandler),

        url(r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/file",
            handlers.ContainerFileHandler, dict(baseUrl=base_url)),
        url(r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/filedata",
            handlers.ContainerFileDataHandler, dict(baseUrl=base_url)),
        url(r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/filedownload",
            handlers.ContainerFileDownloadHandler, dict(baseUrl=base_url)),

        # Topology list and plan handlers
        (r"/topologies/list.json",
         handlers.api.ListTopologiesJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/logicalplan.json",
         handlers.api.TopologyLogicalPlanJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/packingplan.json",
         handlers.api.TopologyPackingPlanJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/physicalplan.json",
         handlers.api.TopologyPhysicalPlanJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/executionstate.json",
         handlers.api.TopologyExecutionStateJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/schedulerlocation.json",
         handlers.api.TopologySchedulerLocationJsonHandler),

        # Counter Handlers
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptions.json",
         handlers.api.TopologyExceptionsJsonHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/exceptionsummary.json",
         handlers.api.TopologyExceptionSummaryHandler),

        # Heron shell Handlers
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/pid",
         handlers.api.PidHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/jstack",
         handlers.api.JstackHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/jmap",
         handlers.api.JmapHandler),
        (r"/topologies/([^\/]+)/([^\/]+)/([^\/]+)/([^\/]+)/histo",
         handlers.api.MemoryHistogramHandler),

        ## Static files
        (r"/static/(.*)", tornado.web.StaticFileHandler,
         dict(path=settings['static_path']))

    ]

    tornado.web.Application.__init__(self, callbacks, **settings)


def define_options(address, port, tracker_url, base_url):
  '''
  :param address:
  :param port:
  :param tracker_url:
  :return:
  '''
  define("address", default=address)
  define("port", default=port)
  define("tracker_url", default=tracker_url)
  define("base_url", default=base_url)


def main():
  '''
  :param argv:
  :return:
  '''
  log.configure(logging.DEBUG)
  tornado.log.enable_pretty_logging()

  # create the parser and parse the arguments
  (parser, child_parser) = args.create_parsers()
  (parsed_args, remaining) = parser.parse_known_args()

  if remaining:
    r = child_parser.parse_args(args=remaining, namespace=parsed_args)
    namespace = vars(r)
    if 'version' in namespace:
      common_config.print_build_info(zipped_pex=True)
    else:
      parser.print_help()
    parser.exit()

  # log additional information
  command_line_args = vars(parsed_args)

  Log.info("Listening at http://%s:%d%s", command_line_args['address'],
           command_line_args['port'], command_line_args['base_url'])
  Log.info("Using tracker url: %s", command_line_args['tracker_url'])

  # pass the options to tornado and start the ui server
  define_options(command_line_args['address'],
                 command_line_args['port'],
                 command_line_args['tracker_url'],
                 command_line_args['base_url'])
  http_server = tornado.httpserver.HTTPServer(Application(command_line_args['base_url']))
  http_server.listen(command_line_args['port'], address=command_line_args['address'])

  # pylint: disable=unused-argument
  # stop Tornado IO loop
  def signal_handler(signum, frame):
    # start a new line after ^C character because this looks nice
    print('\n')
    Log.debug('SIGINT received. Stopping UI')
    tornado.ioloop.IOLoop.instance().stop()

  # associate SIGINT and SIGTERM with a handler
  signal.signal(signal.SIGINT, signal_handler)
  signal.signal(signal.SIGTERM, signal_handler)

  # start Tornado IO loop
  tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
  main()
