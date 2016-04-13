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
  def get(self):
    options = dict(
        topologies = [],               # no topologies
        cluster = 'local',             # default cluster
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
        function = common.className
    )

    # send the single topology page
    self.render("topology.html", **options)

################################################################################
# Handler for displaying the log file for an instance
################################################################################
class LogfileHandler(base.BaseHandler):
  """
  Responsible for creating the web page for files. The html
  will in turn call another endpoint to get the file data.
  """

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, instance):

    options = dict(
        cluster = cluster,
        environ = environ,
        topology = topology,
        instance = instance
    )

    self.render("file.html", **options)

################################################################################
# Handler for getting the data for log file for an instance
################################################################################
class LogfileDataHandler(base.BaseHandler):
  """
  Responsible for getting the data from log file of an instance.
  """

  @tornado.gen.coroutine
  def get(self, cluster, environ, topology, instance, offset, length):

    data = yield access.get_logfile_data(cluster, environ, topology, instance, offset, length)

    self.write(data)
    self.finish()
