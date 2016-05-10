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

import json
import os
import random
import signal
import sys
import time
import tornado.escape
import tornado.web
import tornado.gen
import urllib2
from datetime import datetime

import access
import base
import common
from common.graph import TopologyDAG


################################################################################
# Handler for displaying the config for a topology
################################################################################
class TopologyConfigHandler(base.BaseHandler):
  def get(self, cluster, environ, topology):
    options = dict(
        cluster = cluster,
        environ = environ,
        topology = topology,
        active = "topologies",
        function = common.className)
    self.render("config.html", **options)

################################################################################
# Handler for displaying all the exceptions of a topology
################################################################################
class TopologyExceptionsPageHandler(base.BaseHandler):
  def get(self, cluster, environ, topology, comp_name, instance):
    options = dict(
        cluster = cluster,
        environ = environ,
        topology = topology,
        comp_name = comp_name,
        instance = instance,
        active = "topologies",
        function = common.className)
    # send the exception
    self.render("exception.html", **options)

################################################################################
# Handler for displaying all the topologies - defaults to 'local'
# TO DO: get the list of clusters from tracker and fetch the topologies
################################################################################
class ListTopologiesHandler(base.BaseHandler):
  @tornado.gen.coroutine
  def get(self):
    clusters = yield access.get_clusters()

    options = dict(
        topologies = [],               # no topologies
        clusters = map(str, clusters),
        active = "topologies",         # active icon the nav bar
        function = common.className
    )

    # send the all topologies page
    self.render("topologies.html", **options)

################################################################################
# Handler for displaying the logical plan of a topology
################################################################################
class TopologyPlanHandler(base.BaseHandler):
  @tornado.gen.coroutine
  def get(self, cluster, environ, topology):

    # fetch the execution of the topology asynchronously
    estate = yield access.get_execution_state(cluster, environ, topology)

    # fetch scheduler location of the topology
    scheduler_location = yield access.get_scheduler_location(cluster, environ, topology)

    job_page_link = scheduler_location["job_page_link"]

    # convert the topology launch time to display format
    launched_at = datetime.utcfromtimestamp(estate['submission_time'])
    launched_time = launched_at.strftime('%Y-%m-%d %H:%M:%S UTC')

    options = dict(
        cluster = cluster,
        environ = environ,
        topology = topology,
        estate = estate,
        launched = launched_time,
        status = "running" if random.randint(0,1) else "errors",
        active = "topologies",
        job_page_link = job_page_link,
        function = common.className
    )

    # send the single topology page
    self.render("topology.html", **options)

################################################################################
# Handler for displaying the log file for an instance
################################################################################
class ContainerFileHandler(base.BaseHandler):
  """
  Responsible for creating the web page for files. The html
  will in turn call another endpoint to get the file data.
  """

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):

    path = self.get_argument("path")

    options = dict(
        cluster = cluster,
        environ = environ,
        topology = topology,
        container = container,
        path = path
    )

    self.render("file.html", **options)

################################################################################
# Handler for getting the data for a file in a container of a topology
################################################################################
class ContainerFileDataHandler(base.BaseHandler):
  """
  Responsible for getting the data for a file in a container of a topology.
  """

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):

    offset = self.get_argument("offset")
    length = self.get_argument("length")
    path = self.get_argument("path")

    data = yield access.get_container_file_data(cluster, environ, topology, container, path, offset, length)

    self.write(data)
    self.finish()

################################################################################
# Handler for getting the file stats for a container
################################################################################
class ContainerFileStatsHandler(base.BaseHandler):
  """
  Responsible for getting the file stats for a container.
  """

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):

    path = self.get_argument("path", default=".")
    data = yield access.get_filestats(cluster, environ, topology, container, path)

    options = dict(
        cluster = cluster,
        environ = environ,
        topology = topology,
        container = container,
        path = path,
        filestats = data,
    )
    self.render("browse.html", **options)

################################################################################
# Handler for downloading the file from a container
################################################################################
class ContainerFileDownloadHandler(base.BaseHandler):
  """
  Responsible for downloading the file from a container.
  """

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):

    # If the file is large, we want to abandon downloading
    # if user cancels the requests.
    self.connection_closed = False

    path = self.get_argument("path")
    filename = path.split("/")[-1]
    self.set_header("Content-Disposition", "attachment; filename=%s" % filename)

    # Download the files in chunks. We are downloading from Tracker,
    # which in turns downloads from heron-shell. This much indirection
    # means that if we use static file downloading, the whole files would
    # be cached in memory before it can be sent downstream. Hence, we reuse
    # the file data API to read in chunks until the EOF, or until the download
    # is cancelled by user.

    # 4 MB gives good enough chunk size giving good speed for small files.
    # If files are large, a single threaded download may not be enough.
    length = 4 * 1024 * 1024
    offset = 0
    while True:
      response = yield access.get_container_file_data(cluster, environ, topology, container, path, offset, length)
      if self.connection_closed or 'data' not in response or len(response['data']) < length:
        break
      offset += length
      self.write(response['data'])
      self.flush()

    self.write(response['data'])
    self.finish()

  def on_connection_close(self):
    self.connection_closed = True
