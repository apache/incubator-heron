#!/usr/bin/env python
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


''' killexecutorhandler.py '''
from future.standard_library import install_aliases
install_aliases()

import logging
import os
import signal
from urllib.parse import parse_qsl
import tornado.web

from tornado.options import options

class KillExecutorHandler(tornado.web.RequestHandler):
  """
  Responsible for killing heron-executor process.
  """
  @tornado.web.asynchronous
  def post(self):
    """ post method """
    def status_finish(ret):
      self.set_status(ret)
      self.finish()

    def kill_parent():
      status_finish(200)
      logger.info("Killing parent executor")
      os.killpg(os.getppid(), signal.SIGTERM)

    logger = logging.getLogger(__file__)
    logger.info("Received 'Killing process' request")
    data = dict(parse_qsl(self.request.body))

    # check shared secret
    sharedSecret = data.get('secret')
    if sharedSecret != options.secret:
      status_finish(403)
      return

    instanceId = data.get('instance_id_to_restart')
    if instanceId:
      filepath = instanceId + '.pid'
      if os.path.isfile(filepath): # instance_id found
        if instanceId.startswith('heron-executor-'): # kill heron-executor
          kill_parent()
        else: # kill other normal instance
          fh = open(filepath)
          firstLine = int(fh.readline())
          fh.close()
          logger.info("Killing process " + instanceId + " " + str(firstLine))
          os.kill(firstLine, signal.SIGTERM)
          status_finish(200)
      else: # instance_id not found
        logger.info(filepath + " not found")
        status_finish(422)
    else: # instance_id not given, which means kill the container
      kill_parent()
