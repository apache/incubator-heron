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

''' topology.py '''
import random
from datetime import datetime
import tornado.escape
import tornado.web
import tornado.gen

from . import base
from . import common
import heron.tools.common.src.python.access as access
import heron.common.src.python.utils.log as log

Log = log.Log

################################################################################
# pylint: disable=abstract-method
# pylint: disable=arguments-differ
class TopologyConfigHandler(base.BaseHandler):
  ''' Handler for displaying the config for a topology '''

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  def get(self, cluster, environ, topology):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :return:
    '''
    # pylint: disable=no-member
    options = dict(
        cluster=cluster,
        environ=environ,
        topology=topology,
        active="topologies",
        function=common.className,
        baseUrl=self.baseUrl)
    self.render("config.html", **options)


################################################################################
class TopologyExceptionsPageHandler(base.BaseHandler):
  ''' Handler for displaying all the exceptions of a topology '''

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  def get(self, cluster, environ, topology, comp_name, instance):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :param comp_name:
    :param instance:
    :return:
    '''
    # pylint: disable=no-member
    options = dict(
        cluster=cluster,
        environ=environ,
        topology=topology,
        comp_name=comp_name,
        instance=instance,
        active="topologies",
        function=common.className,
        baseUrl=self.baseUrl)
    # send the exception
    self.render("exception.html", **options)


class ListTopologiesHandler(base.BaseHandler):
  ''' Handler for displaying all the topologies - defaults to 'local'''

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  @tornado.gen.coroutine
  def get(self):
    '''
    :return:
    '''
    clusters = yield access.get_clusters()

    # pylint: disable=no-member
    options = dict(
        topologies=[],  # no topologies
        clusters=[str(cluster) for cluster in clusters],
        active="topologies",  # active icon the nav bar
        function=common.className,
        baseUrl=self.baseUrl
    )

    # send the all topologies page
    self.render("topologies.html", **options)


################################################################################
class TopologyPlanHandler(base.BaseHandler):
  ''' Handler for displaying the logical plan of a topology '''

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :return:
    '''

    # fetch the execution of the topology asynchronously
    execution_state = yield access.get_execution_state(cluster, environ, topology)

    # fetch scheduler location of the topology
    scheduler_location = yield access.get_scheduler_location(cluster, environ, topology)

    job_page_link = scheduler_location["job_page_link"]

    # convert the topology launch time to display format
    launched_at = datetime.utcfromtimestamp(execution_state['submission_time'])
    launched_time = launched_at.strftime('%Y-%m-%d %H:%M:%S UTC')

    # pylint: disable=no-member
    options = dict(
        cluster=cluster,
        environ=environ,
        topology=topology,
        execution_state=execution_state,
        launched=launched_time,
        status="running" if random.randint(0, 1) else "errors",
        active="topologies",
        job_page_link=job_page_link,
        function=common.className,
        baseUrl=self.baseUrl
    )

    # send the single topology page
    self.render("topology.html", **options)


################################################################################
# Handler for displaying the log file for an instance
class ContainerFileHandler(base.BaseHandler):
  """
  Responsible for creating the web page for files. The html
  will in turn call another endpoint to get the file data.
  """

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :param container:
    :return:
    '''
    path = self.get_argument("path")

    options = dict(
        cluster=cluster,
        environ=environ,
        topology=topology,
        container=container,
        path=path,
        baseUrl=self.baseUrl
    )

    self.render("file.html", **options)


################################################################################
# Handler for getting the data for a file in a container of a topology
class ContainerFileDataHandler(base.BaseHandler):
  """
  Responsible for getting the data for a file in a container of a topology.
  """

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :param container:
    :return:
    '''
    offset = self.get_argument("offset")
    length = self.get_argument("length")
    path = self.get_argument("path")

    data = yield access.get_container_file_data(cluster, environ, topology, container, path,
                                                offset, length)

    self.write(data)
    self.finish()


################################################################################
# Handler for getting the file stats for a container
class ContainerFileStatsHandler(base.BaseHandler):
  """
  Responsible for getting the file stats for a container.
  """

  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :param container:
    :return:
    '''
    path = self.get_argument("path", default=".")
    data = yield access.get_filestats(cluster, environ, topology, container, path)

    options = dict(
        cluster=cluster,
        environ=environ,
        topology=topology,
        container=container,
        path=path,
        filestats=data,
        baseUrl=self.baseUrl)
    self.render("browse.html", **options)


################################################################################
# Handler for downloading the file from a container
class ContainerFileDownloadHandler(base.BaseHandler):
  """
  Responsible for downloading the file from a container.
  """
  def initialize(self, baseUrl):
    self.baseUrl = baseUrl

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, container):
    '''
    :param cluster:
    :param environ:
    :param topology:
    :param container:
    :return:
    '''
    # If the file is large, we want to abandon downloading
    # if user cancels the requests.
    # pylint: disable=attribute-defined-outside-init
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
    file_download_url = access.get_container_file_download_url(cluster, environ,
                                                               topology, container, path)

    Log.debug("file download url: %s", str(file_download_url))
    def streaming_callback(chunk):
      self.write(chunk)
      self.flush()

    http_client = tornado.httpclient.AsyncHTTPClient()
    yield http_client.fetch(file_download_url, streaming_callback=streaming_callback)
    self.finish()


  def on_connection_close(self):
    '''
    :return:
    '''
    # pylint: disable=attribute-defined-outside-init
    self.connection_closed = True
