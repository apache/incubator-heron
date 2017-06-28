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
import tornado.web

class KillExecutorHandler(tornado.web.RequestHandler):
  """
  Responsible for killing heron-executor process.
  """
  @tornado.web.asynchronous
  def post(self):
    """ post method """
    logger = logging.getLogger(__file__)
    logger.info("Received 'Killing parent executor' request")
    # todo(huijun): add security check
    logger.info("Killing parent executor response 200")
    self.set_status(200)
    self.finish()
    logger.info("Killing parent executor")
    os.killpg(os.getppid(), signal.SIGTERM)
