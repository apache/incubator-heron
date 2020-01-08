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


''' filehandler.py '''
import tornado.web
from tornado.template import Template

from heron.shell.src.python import utils

class FileHandler(tornado.web.RequestHandler):
  """
  Responsible for creating the web page for files. The html
  will in turn call the /filedata/ endpoint to get the file data.
  """
  @tornado.web.asynchronous
  def get(self, path):
    """ get method """
    t = Template(utils.get_asset("file.html"))
    if path is None:
      self.set_status(404)
      self.write("No such file")
      self.finish()
      return

    if not utils.check_path(path):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return

    args = dict(
        filename=path,
        jquery=utils.get_asset("jquery.js"),
        pailer=utils.get_asset("jquery.pailer.js"),
        css=utils.get_asset("bootstrap.css"),
    )
    self.write(t.generate(**args))
    self.finish()
