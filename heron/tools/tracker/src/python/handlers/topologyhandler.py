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

''' topologyhandler.py '''
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python.handlers import BaseHandler


class TopologyHandler(BaseHandler):
  """
  url - /topologies/info
  Parameters:
   - cluster (required)
   - role - (optional) Role used to submit the topology.
   - environ (required)
   - topology (required) name of the requested topology

  the response json is a dictionary with all the
  information of the topology, including its
  logical and physical plan.
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
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)
      self.write_success_response(topology_info)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
