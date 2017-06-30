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

#!/usr/bin/env python2.7
''' main.py '''
import logging
import tornado.ioloop
import tornado.web

from tornado.httpclient import AsyncHTTPClient
from tornado.options import define, options, parse_command_line

from heron.shell.src.python import handlers

AsyncHTTPClient.configure(None, defaults=dict(request_timeout=120.0))
app = tornado.web.Application([
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
])


if __name__ == '__main__':
  define("port", default=9999, help="Runs on the given port", type=int)
  define("secret", default='', help="Shared secret for /killexecutor", type=str)
  parse_command_line()

  logger = logging.getLogger(__file__)
  logger.info("Starting Heron Shell")
  logger.info("Shared secret for /killexecutor: %s", options.secret)

  app.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()
