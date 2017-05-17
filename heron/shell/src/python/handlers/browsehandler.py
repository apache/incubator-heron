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

''' browsehandler.py '''
import os
import stat
import tornado.web
from tornado.template import Template

from heron.shell.src.python import utils

class BrowseHandler(tornado.web.RequestHandler):
  """
  Responsible for browsing directories.
  """

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, path):
    ''' get method '''
    if not path:
      path = "."
    if path.startswith("/"):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return
    t = Template(utils.get_asset("browse.html"))
    args = dict(
        path=path,
        listing=utils.get_listing(path),
        format_prefix=utils.format_prefix,
        stat=stat,
        get_stat=utils.get_stat,
        os=os,
        css=utils.get_asset("bootstrap.css")
    )
    self.write(t.generate(**args))
    self.finish()
