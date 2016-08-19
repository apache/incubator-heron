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
''' base.py '''
import tornado.ioloop
import tornado.web
import tornado.httpserver


# pylint: disable=abstract-method
class BaseHandler(tornado.web.RequestHandler):
  ''' BaseHandler '''

  def write_error(self, status_code, **kwargs):
    '''
    :param status_code:
    :param kwargs:
    :return:
    '''
    if "exc_info" in kwargs:
      exc_info = kwargs["exc_info"]
      error = exc_info[1]

      errormessage = "%s: %s" % (status_code, error)
      self.render("error.html", errormessage=errormessage)
    else:
      errormessage = "%s" % (status_code)
      self.render("error.html", errormessage=errormessage)
