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

''' memoryhistogramhandler.py '''
import json
import tornado.web

from heron.shell.src.python import utils

class MemoryHistogramHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the memory histogram of a jvm process given its pid.
  """

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, pid):
    ''' get method '''
    body = utils.str_cmd(['jmap', '-histo', pid])
    self.content_type = 'application/json'
    self.write(json.dumps(body))
    self.finish()
