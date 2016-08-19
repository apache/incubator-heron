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
""" defaulthandler.py """
import tornado.gen

from heron.tools.tracker.src.python.handlers import BaseHandler

class DefaultHandler(BaseHandler):
  """
  URL - anything that is not supported

  This is the default case in the regular expression
  matching for the URLs. If nothin matched before this,
  then this is the URL that is not supported by the API.

  Sends back a "failure" response to the client.
  """

  @tornado.gen.coroutine
  def get(self, url):
    """ get method """
    self.write_error_response("URL not supported: " + url)
