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
import os
import logging
import tornado.web
import anticrlf

from heron.shell.src.python import utils

class DownloadHandler(tornado.web.RequestHandler):
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

    # If the file is large, we want to abandon downloading
    # if user cancels the requests.
    # pylint: disable=attribute-defined-outside-init
    self.connection_closed = False

    self.set_header("Content-Disposition", "attachment")
    if not utils.check_path(path):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return

    if path is None or not os.path.isfile(path):
      self.write(f"File {path} not found")
      self.set_status(404)
      self.finish()
      return

    length = int(4 * 1024 * 1024)
    offset = int(0)
    while True:
      data = await utils.read_chunk(path, offset=offset, length=length, escape_data=False)
      if self.connection_closed or 'data' not in data or len(data['data']) < length:
        break
      offset += length
      self.write(data['data'])
      self.flush()

    if 'data' in data:
      self.write(data['data'])
    self.finish()

def on_connection_close(self):
  '''
  :return:
  '''
  # pylint: disable=attribute-defined-outside-init
  self.connection_closed = True
