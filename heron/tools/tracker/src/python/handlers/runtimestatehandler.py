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

''' runtimestatehandler.py '''
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.proto import tmaster_pb2
from heron.tools.tracker.src.python.handlers import BaseHandler

# pylint: disable=attribute-defined-outside-init
class RuntimeStateHandler(BaseHandler):
  """
  URL - /topologies/runtimestate
  Parameters:
   - cluster (required)
   - environ (required)
   - role - (optional) Role used to submit the topology.
   - topology (required) name of the requested topology

  The response JSON is a dictionary with all the
  runtime information of a topology. Static properties
  is availble from /topologies/metadata.

  Example JSON response:
    {
      has_tmaster_location: true,
      stmgrs_reg_summary: {
        registered_stmgrs: [
          "stmgr-1",
          "stmgr-2"
        ],
        absent_stmgrs: [ ]
      },
      has_scheduler_location: true,
      has_physical_plan: true
    }
  """
  def initialize(self, tracker):
    """ initialize """
    self.tracker = tracker

  # pylint: disable=dangerous-default-value, no-self-use, unused-argument
  @tornado.gen.coroutine
  def getStmgrsRegSummary(self, tmaster, callback=None):
    """
    Get summary of stream managers registration summary
    """
    if not tmaster or not tmaster.host or not tmaster.stats_port:
      return
    reg_request = tmaster_pb2.StmgrsRegistrationSummaryRequest()
    request_str = reg_request.SerializeToString()
    port = str(tmaster.stats_port)
    host = tmaster.host
    url = "http://{0}:{1}/stmgrsregistrationsummary".format(host, port)
    request = tornado.httpclient.HTTPRequest(url,
                                             body=request_str,
                                             method='POST',
                                             request_timeout=5)
    Log.debug('Making HTTP call to fetch stmgrsregistrationsummary url: %s', url)
    try:
      client = tornado.httpclient.AsyncHTTPClient()
      result = yield client.fetch(request)
      Log.debug("HTTP call complete.")
    except tornado.httpclient.HTTPError as e:
      raise Exception(str(e))
    # Check the response code - error if it is in 400s or 500s
    responseCode = result.code
    if responseCode >= 400:
      message = "Error in getting exceptions from Tmaster, code: " + responseCode
      Log.error(message)
      raise tornado.gen.Return({
          "message": message
      })
    # Parse the response from tmaster.
    reg_response = tmaster_pb2.StmgrsRegistrationSummaryResponse()
    reg_response.ParseFromString(result.body)
    # Send response
    ret = {}
    for stmgr in reg_response.registered_stmgrs:
      ret[stmgr] = True
    for stmgr in reg_response.absent_stmgrs:
      ret[stmgr] = False
    raise tornado.gen.Return(ret)

  @tornado.gen.coroutine
  def get(self):
    """ get method """
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)
      runtime_state = topology_info["runtime_state"]
      runtime_state["topology_version"] = topology_info["metadata"]["release_version"]
      topology = self.tracker.getTopologyByClusterRoleEnvironAndName(
          cluster, role, environ, topology_name)
      reg_summary = yield tornado.gen.Task(self.getStmgrsRegSummary, topology.tmaster)
      for stmgr, reg in list(reg_summary.items()):
        runtime_state["stmgrs"].setdefault(stmgr, {})["is_registered"] = reg
      self.write_success_response(runtime_state)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
