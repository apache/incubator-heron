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

''' killexecutorhandler.py '''
import logging
import os
import signal
import urlparse
import tornado.web

from tornado.options import options

class KillExecutorHandler(tornado.web.RequestHandler):
  """
  Responsible for killing heron-executor process.
  """
  @tornado.web.asynchronous
  def post(self):
    """ post method """
    logger = logging.getLogger(__file__)
    logger.info("Received 'Killing parent executor' request")
    data = dict(urlparse.parse_qsl(self.request.body))

    def status_finish(ret):
      self.set_status(ret)
      self.finish()

    sharedSecret = data.get('secret')
    if sharedSecret != options.secret:
      status_finish(403)
      return

    def kill_parent():
      status_finish(200)
      logger.info("Killing parent executor")
      os.killpg(os.getppid(), signal.SIGTERM)

    processName = data.get('process')
    if processName:
      filepath = processName + '.pid'
      if os.path.isfile(filepath):
        if processName.startswith('heron-executor-'):
          kill_parent()
        else:
          firstLine = open(filepath).readline()
          logger.info("Killing process " + processName + " " + firstLine)
          os.killpg(int(firstLine), signal.SIGTERM)
          status_finish(200)
      else:
        logger.info(filepath + " not found")
        status_finish(422)
    else:
      kill_parent()
