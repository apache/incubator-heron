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

''' metadatahandler.py '''
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python.handlers import BaseHandler

# pylint: disable=attribute-defined-outside-init
class MetaDataHandler(BaseHandler):
  """
  URL - /topologies/metadata
  Parameters:
   - cluster (required)
   - environ (required)
   - role - (optional) Role used to submit the topology.
   - topology (required) name of the requested topology

  The response JSON is a dictionary with all the
  static properties of a topology. Runtime information
  is available from /topologies/runtimestate.

  Example JSON response:
    {
      release_version: "foo/bar",
      cluster: "local",
      release_tag: "",
      environ: "default",
      submission_user: "foo",
      release_username: "foo",
      submission_time: 1489523952,
      viz: "",
      role: "foo",
      jobname: "EX"
    }
  """

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
      metadata = topology_info["metadata"]
      self.write_success_response(metadata)
    except Exception as e:
      Log.error("Exception when handling GET request '/topologies/metadata'")
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
