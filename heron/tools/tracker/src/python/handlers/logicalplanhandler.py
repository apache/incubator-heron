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

""" logicalplanhandler.py """
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python import graph
from heron.tools.tracker.src.python.handlers import BaseHandler


class LogicalPlanHandler(BaseHandler):
  """
  URL - /topologies/logicalplan
  Parameters:
   - cluster (required)
   - role - (role) Role used to submit the topology.
   - environ (required)
   - topology (required) name of the requested topology

  The response JSON is a dictionary with all the
  information of logical plan of the topology.
  """
  # pylint: disable=missing-docstring, attribute-defined-outside-init
  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)
      lplan = topology_info["logical_plan"]

      # format the logical plan as required by the web (because of Ambrose)
      # first, spouts followed by bolts
      spouts_map = dict()
      for name, value in list(lplan['spouts'].items()):
        spouts_map[name] = dict(
            config=value.get("config", dict()),
            outputs=value["outputs"],
            spout_type=value["type"],
            spout_source=value["source"],
            extra_links=value["extra_links"],
        )

      bolts_map = dict()
      for name, value in list(lplan['bolts'].items()):
        bolts_map[name] = dict(
            config=value.get("config", dict()),
            inputComponents=[i['component_name'] for i in value['inputs']],
            inputs=value["inputs"],
            outputs=value["outputs"]
        )

      diameter = graph.TopologyDAG(lplan).diameter()

      result = dict(
          stages=diameter,
          spouts=spouts_map,
          bolts=bolts_map
      )

      self.write_success_response(result)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
