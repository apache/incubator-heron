#!/usr/bin/env python3
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
import logging
import tornado.ioloop
import tornado.web

from tornado.httpclient import AsyncHTTPClient
from tornado.options import define, options, parse_command_line

from heron.shell.src.python import handlers

default_handlers = [
    (r"^/jmap/([0-9]+$)", handlers.JmapHandler),
    (r"^/histo/([0-9]+$)", handlers.MemoryHistogramHandler),
    (r"^/pmap/([0-9]+$)", handlers.PmapHandler),
    (r"^/jstack/([0-9]+$)", handlers.JstackHandler),
    (r"^/pid/(.*)", handlers.PidHandler),
    (r"^/browse/(.*)", handlers.BrowseHandler),
    (r"^/file/(.*)", handlers.FileHandler),
    (r"^/filedata/(.*)", handlers.FileDataHandler),
    (r"^/filestats/(.*)", handlers.FileStatsHandler),
    (r"^/download/(.*)", handlers.DownloadHandler),
    (r"^/killexecutor", handlers.KillExecutorHandler),
    (r"^/quitquitquit", handlers.KillExecutorHandler),
    (r"^/abortabortabort", handlers.KillExecutorHandler),
    (r"^/health", handlers.HealthHandler),
]

# pylint: disable=dangerous-default-value
def run(url_to_handlers=default_handlers):
  define("port", default=9999, help="Runs on the given port", type=int)
  define("secret", default='', help="Shared secret for /killexecutor", type=str)
  parse_command_line()

  logger = logging.getLogger(__file__)
  logger.info("Starting Heron Shell")
  logger.info("Shared secret for /killexecutor: %s", options.secret)

  AsyncHTTPClient.configure(None, defaults=dict(request_timeout=120.0))
  app = tornado.web.Application(url_to_handlers)
  app.listen(options.port)
  tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
  run()
