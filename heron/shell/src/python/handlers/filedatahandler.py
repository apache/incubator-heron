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

''' filedatahandler.py '''
import json
import os
import tornado.web

from heron.shell.src.python import utils

class FileDataHandler(tornado.web.RequestHandler):
  """
  Responsible for reading and returning the file data given the offset
  and length of file to be read.
  """
  @tornado.web.asynchronous
  def get(self, path):
    """ get method """
    if path is None:
      return {}
    if path.startswith("/"):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return
    offset = self.get_argument("offset", default=-1)
    length = self.get_argument("length", default=-1)
    if not os.path.isfile(path):
      return {}
    data = utils.read_chunk(path, offset, length)
    self.write(json.dumps(data))
    self.finish()
