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

''' metrics.py '''
from heron.tools.ui.src.python.handlers import base
import heron.tools.common.src.python.access as access

import tornado.gen

ALL_INSTANCES = '*'

# pylint: disable=invalid-name
query_handler = access.HeronQueryHandler()


class MetricsHandler(base.BaseHandler):
  ''' MetricsHandler '''

  @tornado.gen.coroutine
  def get(self):
    '''
    :return:
    '''
    cluster = self.get_argument("cluster")
    environ = self.get_argument("environ")
    topology = self.get_argument("topology")
    component = self.get_argument("component", default=None)
    metricnames = self.get_arguments("metricname")
    instances = self.get_arguments("instance")
    interval = self.get_argument("interval", default=-1)
    time_range = (0, interval)
    compnames = [component] if component else (yield access.get_comps(cluster, environ, topology))

    # fetch the metrics
    futures = {}
    for comp in compnames:
      future = access.get_comp_metrics(
          cluster, environ, topology, comp, instances,
          metricnames, time_range)
      futures[comp] = future

    results = yield futures

    self.write(results[component] if component else results)


class MetricsTimelineHandler(base.BaseHandler):
  ''' MetricsTimelineHandler '''

  @tornado.gen.coroutine
  def get(self):
    '''
    :return:
    '''
    cluster = self.get_argument("cluster")
    environ = self.get_argument("environ")
    topology = self.get_argument("topology")
    component = self.get_argument("component", default=None)
    metric = self.get_argument("metric")
    instances = self.get_argument("instance")
    start = self.get_argument("starttime")
    end = self.get_argument("endtime")
    maxquery = self.get_argument("max", default=False)
    timerange = (start, end)
    compnames = [component]

    # fetch the metrics
    futures = {}
    if metric == "backpressure":
      for comp in compnames:
        future = query_handler.fetch_backpressure(cluster, metric, topology, component,
                                                  instances, timerange, maxquery, environ)
        futures[comp] = future
    else:
      fetch = query_handler.fetch_max if maxquery else query_handler.fetch
      for comp in compnames:
        future = fetch(cluster, metric, topology, component,
                       instances, timerange, environ)
        futures[comp] = future

    results = yield futures
    self.write(results[component] if component else results)
