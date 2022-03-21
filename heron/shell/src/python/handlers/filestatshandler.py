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


''' filestatshandler.py '''
import json
import os
import stat
import tornado.web

from heron.shell.src.python import utils

class FileStatsHandler(tornado.web.RequestHandler):
  """
  Get the file stats in JSON format given the path.
  """
  async def get(self, path):
    ''' get method '''
    path = tornado.escape.url_unescape(path)
    if not path:
      path = "."

    # User should not be able to access anything outside
    # of the dir that heron-shell is running in. This ensures
    # sandboxing. So we don't allow absolute paths and parent
    # accessing.
    if not utils.check_path(path):
      self.set_status(403)
      await self.finish("Only relative paths are allowed")
      return

    listing = utils.get_listing(path)
    file_stats = {}
    for fn in listing:
      try:
        is_dir = False
        formatted_stat = utils.format_prefix(fn, utils.get_stat(path, fn))
        if stat.S_ISDIR(utils.get_stat(path, fn).st_mode):
          is_dir = True
        file_stats[fn] = {
            "formatted_stat": formatted_stat,
            "is_dir": is_dir,
            "path": tornado.escape.url_escape(os.path.join(path, fn)),
        }
        if fn == "..":
          path_fragments = path.split("/")
          if not path_fragments:
            file_stats[fn]["path"] = "."
          else:
            file_stats[fn]["path"] = tornado.escape.url_escape("/".join(path_fragments[:-1]))
      except:
        continue
    await self.finish(json.dumps(file_stats))
