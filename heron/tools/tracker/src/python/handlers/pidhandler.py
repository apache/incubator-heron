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

''' pidhandler.py '''
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python import utils
from heron.tools.tracker.src.python.handlers import BaseHandler


@tornado.gen.coroutine
def getInstancePid(topology_info, instance_id):
  """
  This method is used by other modules, and so it
  is not a part of the class.
  Fetches Instance pid from heron-shell.
  """
  try:
    http_client = tornado.httpclient.AsyncHTTPClient()
    endpoint = utils.make_shell_endpoint(topology_info, instance_id)
    url = "%s/pid/%s" % (endpoint, instance_id)
    Log.debug("HTTP call for url: %s", url)
    response = yield http_client.fetch(url)
    raise tornado.gen.Return(response.body)
  except tornado.httpclient.HTTPError as e:
    raise Exception(str(e))


class PidHandler(BaseHandler):
  """
  URL - /topologies/jmap?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&instance=<instance>
  Parameters:
   - cluster - Name of the cluster.
   - role - (optional) Role used to submit the topology.
   - environ - Running environment.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - instance - Instance Id

  If successfule returns the pid of instance. May include training
  spaces and/or linefeed before/after.
  The response JSON is a dict with following format:
  {
     'command': Full command executed at server.
     'stdout': Text on stdout of executing the command.
     'stderr': <optional> Text on stderr.
  }
  """

  # pylint: disable=attribute-defined-outside-init
  def initialize(self, tracker):
    """ initialize """
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    """ get method """
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      instance = self.get_argument_instance()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)
      result = yield getInstancePid(topology_info, instance)
      self.write_success_response(result)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
