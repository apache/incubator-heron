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


''' downloadhandler.py '''
import mimetypes
import os
import logging
from tornado import web, iostream, gen
import anticrlf

from heron.shell.src.python import utils

class DownloadHandler(web.RequestHandler):
  """
  Responsible for downloading the files.
  """
  async def get(self, path):
    """ get method """

    handler = logging.StreamHandler()
    handler.setFormatter(anticrlf.LogFormatter('%(levelname)s:%(name)s:%(message)s'))
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.debug("request to download: %s", path)

    self.set_header("Content-Disposition", "attachment")
    if not utils.check_path(path):
      self.set_status(403)
      await self.finish("Only relative paths are allowed")
      return

    if path is None or not os.path.isfile(path):
      self.set_status(404)
      await self.finish("File %s  not found" % path)
      return

    chunk_size = int(4 * 1024 * 1024)
    content_type = mimetypes.guess_type(path)
    self.set_header("Content-Type", content_type[0])
    with open(path, 'rb') as f:
      while True:
        chunk = f.read(chunk_size)
        if not chunk:
          break
        try:
          self.write(chunk)  # write the chunk to response
          await self.flush() # send the chunk to client
        except iostream.StreamCloseError:
          # this means the client has closed the connection
          # so break the loop
          break
        finally:
          # deleting the chunk is very important because
          # if many client are downloading files at the
          # same time, the chunks in memory will keep
          # increasing and will eat up the RAM
          del chunk
          # pause the coroutine so other handlers can run
          await gen.sleep(0.000000001) # 1 nanosecond
