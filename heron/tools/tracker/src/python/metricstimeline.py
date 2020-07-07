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

""" metricstimeline.py """
import tornado.gen

from heron.common.src.python.utils.log import Log
from heron.proto import common_pb2
from heron.proto import tmaster_pb2

# pylint: disable=too-many-locals, too-many-branches, unused-argument
@tornado.gen.coroutine
def getMetricsTimeline(tmaster,
                       component_name,
                       metric_names,
                       instances,
                       start_time,
                       end_time,
                       callback=None):
  """
  Get the specified metrics for the given component name of this topology.
  Returns the following dict on success:
  {
    "timeline": {
      <metricname>: {
        <instance>: {
          <start_time> : <numeric value>,
          <start_time> : <numeric value>,
          ...
        }
        ...
      }, ...
    },
    "starttime": <numeric value>,
    "endtime": <numeric value>,
    "component": "..."
  }

  Returns the following dict on failure:
  {
    "message": "..."
  }
  """
  # Tmaster is the proto object and must have host and port for stats.
  if not tmaster or not tmaster.host or not tmaster.stats_port:
    raise Exception("No Tmaster found")

  host = tmaster.host
  port = tmaster.stats_port

  # Create the proto request object to get metrics.

  metricRequest = tmaster_pb2.MetricRequest()
  metricRequest.component_name = component_name

  # If no instances are give, metrics for all instances
  # are fetched by default.
  if len(instances) > 0:
    for instance in instances:
      metricRequest.instance_id.append(instance)

  for metricName in metric_names:
    metricRequest.metric.append(metricName)

  metricRequest.explicit_interval.start = start_time
  metricRequest.explicit_interval.end = end_time
  metricRequest.minutely = True

  # Serialize the metricRequest to send as a payload
  # with the HTTP request.
  metricRequestString = metricRequest.SerializeToString()

  # Form and send the http request.
  url = "http://{0}:{1}/stats".format(host, port)
  request = tornado.httpclient.HTTPRequest(url,
                                           body=metricRequestString,
                                           method='POST',
                                           request_timeout=5)

  Log.debug("Making HTTP call to fetch metrics")
  Log.debug("url: " + url)
  try:
    client = tornado.httpclient.AsyncHTTPClient()
    result = yield client.fetch(request)
    Log.debug("HTTP call complete.")
  except tornado.httpclient.HTTPError as e:
    raise Exception(str(e))


  # Check the response code - error if it is in 400s or 500s
  responseCode = result.code
  if responseCode >= 400:
    message = "Error in getting metrics from Tmaster, code: " + responseCode
    Log.error(message)
    raise Exception(message)

  # Parse the response from tmaster.
  metricResponse = tmaster_pb2.MetricResponse()
  metricResponse.ParseFromString(result.body)

  if metricResponse.status.status == common_pb2.NOTOK:
    if metricResponse.status.HasField("message"):
      Log.warn("Received response from Tmaster: %s", metricResponse.status.message)

  # Form the response.
  ret = {}
  ret["starttime"] = start_time
  ret["endtime"] = end_time
  ret["component"] = component_name
  ret["timeline"] = {}

  # Loop through all the metrics
  # One instance corresponds to one metric, which can have
  # multiple IndividualMetrics for each metricname requested.
  for metric in metricResponse.metric:
    instance = metric.instance_id

    # Loop through all individual metrics.
    for im in metric.metric:
      metricname = im.name
      if metricname not in ret["timeline"]:
        ret["timeline"][metricname] = {}
      if instance not in ret["timeline"][metricname]:
        ret["timeline"][metricname][instance] = {}

      # We get minutely metrics.
      # Interval-values correspond to the minutely mark for which
      # this metric value corresponds to.
      for interval_value in im.interval_values:
        ret["timeline"][metricname][instance][interval_value.interval.start] = interval_value.value

  raise tornado.gen.Return(ret)
