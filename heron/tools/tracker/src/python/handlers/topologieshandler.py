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

''' topologieshandler.py '''
import tornado.gen

from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python.handlers import BaseHandler


class TopologiesHandler(BaseHandler):
  """
  URL - /topologies
  Parameters:
   - cluster (optional)
   - tag (optional)

  The response JSON is a dict with following format:
  {
    <cluster1>: {
      <default>: [
        topology1,
        topology2,
        ...
      ],
      <environ1>: [
        topology1,
        topology2,
        ...
      ],
      <environ2>: [...],
      ...
    },
    <cluster2>: {...}
  }
  """
  # pylint: disable=attribute-defined-outside-init
  def initialize(self, tracker):
    """ initialize """
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    """ get method """
    # Get all the values for parameter "cluster".
    clusters = self.get_arguments(constants.PARAM_CLUSTER)
    # Get all the values for parameter "environ".
    environs = self.get_arguments(constants.PARAM_ENVIRON)
    # Get role
    role = self.get_argument_role()

    ret = {}
    topologies = self.tracker.topologies
    for topology in topologies:
      cluster = topology.cluster
      environ = topology.environ
      execution_state = topology.execution_state

      if not cluster or not execution_state or not environ:
        continue

      topo_role = execution_state.role
      if not topo_role:
        continue

      # This cluster is not asked for.
      # Note that "if not clusters", then
      # we show for all the clusters.
      if clusters and cluster not in clusters:
        continue

      # This environ is not asked for.
      # Note that "if not environs", then
      # we show for all the environs.
      if environs and environ not in environs:
        continue

      # This role is not asked for.
      # Note that "if not role", then
      # we show for all the roles.
      if role and role != topo_role:
        continue

      if cluster not in ret:
        ret[cluster] = {}
      if topo_role not in ret[cluster]:
        ret[cluster][topo_role] = {}
      if environ not in ret[cluster][topo_role]:
        ret[cluster][topo_role][environ] = []
      ret[cluster][topo_role][environ].append(topology.name)
    self.write_success_response(ret)
