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

''' containerfilehandler.py '''
import json
import traceback
import tornado.gen

from heron.common.src.python.utils.log import Log
from heron.tools.tracker.src.python.handlers import BaseHandler
from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python import utils


# pylint: disable=attribute-defined-outside-init
class ContainerFileDataHandler(BaseHandler):
  """
  URL - /topologies/containerfiledata?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&container=<container>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
   - role - (optional) Role used to submit the topology.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - container - Container number
   - path - Relative path to the file
   - offset - From which to read the file
   - length - How much data to read

  Get the data from the file for the given topology, container
  and path. The data being read is based on offset and length.
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
      container = self.get_argument(constants.PARAM_CONTAINER)
      path = self.get_argument(constants.PARAM_PATH)
      offset = self.get_argument_offset()
      length = self.get_argument_length()
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)

      stmgr_id = "stmgr-" + container
      stmgr = topology_info["physical_plan"]["stmgrs"][stmgr_id]
      host = stmgr["host"]
      shell_port = stmgr["shell_port"]
      file_data_url = "http://%s:%d/filedata/%s?offset=%s&length=%s" % \
        (host, shell_port, path, offset, length)

      http_client = tornado.httpclient.AsyncHTTPClient()
      response = yield http_client.fetch(file_data_url)
      self.write_success_response(json.loads(response.body))
      self.finish()
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)

# pylint: disable=attribute-defined-outside-init
class ContainerFileDownloadHandler(BaseHandler):
  """
  URL - /topologies/containerfiledownload?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&container=<container>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
   - role - (optional) Role used to submit the topology.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - container - Container number
   - path - Relative path to the file

  Download the file for the given topology, container and path.
  """
  def initialize(self, tracker):
    """ initialize """
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    try:
      cluster = self.get_argument_cluster()
      role = self.get_argument_role()
      environ = self.get_argument_environ()
      topology_name = self.get_argument_topology()
      container = self.get_argument(constants.PARAM_CONTAINER)
      path = self.get_argument(constants.PARAM_PATH)
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)

      stmgr_id = "stmgr-" + container
      stmgr = topology_info["physical_plan"]["stmgrs"][stmgr_id]
      host = stmgr["host"]
      shell_port = stmgr["shell_port"]
      file_download_url = "http://%s:%d/download/%s" % (host, shell_port, path)
      Log.debug("download file url: %s", file_download_url)

      path = self.get_argument("path")
      filename = path.split("/")[-1]
      self.set_header("Content-Disposition", "attachment; filename=%s" % filename)

      def streaming_callback(chunk):
        self.write(chunk)
        self.flush()

      http_client = tornado.httpclient.AsyncHTTPClient()
      yield http_client.fetch(file_download_url, streaming_callback=streaming_callback)
      self.finish()
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)


class ContainerFileStatsHandler(BaseHandler):
  """
  URL - /topologies/containerfilestats?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&container=<container>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
   - role - (optional) Role used to submit the topology.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - container - Container number
   - path - Relative path to the directory

  Get the dir stats for the given topology, container and path.
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
      container = self.get_argument(constants.PARAM_CONTAINER)
      path = self.get_argument(constants.PARAM_PATH, default=".")
      topology_info = self.tracker.getTopologyInfo(topology_name, cluster, role, environ)

      stmgr_id = "stmgr-" + str(container)
      stmgr = topology_info["physical_plan"]["stmgrs"][stmgr_id]
      host = stmgr["host"]
      shell_port = stmgr["shell_port"]
      filestats_url = utils.make_shell_filestats_url(host, shell_port, path)

      http_client = tornado.httpclient.AsyncHTTPClient()
      response = yield http_client.fetch(filestats_url)
      self.write_success_response(json.loads(response.body))
      self.finish()
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)
