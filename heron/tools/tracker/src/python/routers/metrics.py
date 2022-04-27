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
"""
Views on Heron metrics.

"""
from typing import Dict, List, Optional

import httpx

from fastapi import Query, APIRouter
from pydantic import BaseModel, Field

from heron.common.src.python.utils.log import Log
from heron.proto import common_pb2
from heron.proto import tmanager_pb2
from heron.tools.tracker.src.python import metricstimeline, state
from heron.tools.tracker.src.python.query import Query as TManagerQuery
from heron.tools.tracker.src.python.utils import BadRequest

router = APIRouter()

class ComponentMetrics(BaseModel):
  interval: int
  component: str
  metrics: Dict[str, Dict[str, str]] = Field(
      ...,
      description="a map of (metric, instance) to value"
  )


async def get_component_metrics(
    tmanager,
    component: str,
    metric_names: List[str],
    instances: List[str],
    interval: int,
) -> ComponentMetrics:
  """
  Return metrics from the Tmanager over the given interval.

  The metrics property is keyed with (metric, instance) to metric values.

  Metrics not included in `metric_names` will be truncated.

  """
  if not (tmanager and tmanager.host and tmanager.stats_port):
    raise Exception("No Tmanager found")

  metric_request = tmanager_pb2.MetricRequest()
  metric_request.component_name = component
  if instances:
    metric_request.instance_id.extend(instances)
  metric_request.metric.extend(metric_names)
  metric_request.interval = interval
  url = f"http://{tmanager.host}:{tmanager.stats_port}/stats"
  async with httpx.AsyncClient() as client:
    response = await client.post(url, data=metric_request.SerializeToString())
  metric_response = tmanager_pb2.MetricResponse()
  metric_response.ParseFromString(response.content)

  if metric_response.status.status == common_pb2.NOTOK:
    if metric_response.status.HasField("message"):
      Log.warn(
          "Received response from Tmanager: %s", metric_response.status.message
      )

  metrics = {}
  for metric in metric_response.metric:
    instance = metric.instance_id
    for instance_metric in metric.metric:
      metrics.setdefault(instance_metric.name, {})[
          instance
      ] = instance_metric.value

  return ComponentMetrics(
      interval=metric_response.interval,
      component=component,
      metrics=metrics,
  )


@router.get("/metrics", response_model=ComponentMetrics)
async def get_metrics( # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    component: str,
    role: Optional[str] = None,
    topology_name: str = Query(..., alias="topology"),
    metric_names: Optional[List[str]] = Query(None, alias="metricname"),
    instances: Optional[List[str]] = Query(None, alias="instance"),
    interval: int = -1,
):
  """
  Return metrics over the given interval. Metrics not included in `metric_names`
  will be truncated.

  """
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  return await get_component_metrics(
      topology.tmanager, component, metric_names, instances, interval
  )


@router.get("/metrics/timeline", response_model=metricstimeline.MetricsTimeline)
async def get_metrics_timeline( # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    component: str,
    start_time: int = Query(..., alias="starttime"),
    end_time: int = Query(..., alias="endtime"),
    role: Optional[str] = None,
    topology_name: str = Query(..., alias="topology"),
    metric_names: Optional[List[str]] = Query(None, alias="metricname"),
    instances: Optional[List[str]] = Query(None, alias="instance"),
):
  """
  '/metrics/timeline' 0.20.5 above.
  Return metrics over the given interval.
  """
  if start_time > end_time:
    raise BadRequest("start_time > end_time")
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  return await metricstimeline.get_metrics_timeline(
      topology.tmanager, component, metric_names, instances, start_time, end_time
  )


@router.get("/metricstimeline", response_model=metricstimeline.LegacyMetricsTimeline,
    deprecated=True)
async def get_legacy_metrics_timeline(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    component: str,
    start_time: int = Query(..., alias="starttime"),
    end_time: int = Query(..., alias="endtime"),
    role: Optional[str] = None,
    topology_name: str = Query(..., alias="topology"),
    metric_names: Optional[List[str]] = Query(None, alias="metricname"),
    instances: Optional[List[str]] = Query(None, alias="instance"),
):
  """
  '/metricstimeline' 0.20.5 below.
  Return metrics over the given interval.
  """
  if start_time > end_time:
    raise BadRequest("start_time > end_time")
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  return await metricstimeline.get_metrics_timeline(
      topology.tmanager, component, metric_names, instances, start_time, end_time
  )


class TimelinePoint(BaseModel): # pylint: disable=too-few-public-methods
  """A metric at discrete points in time."""
  instance: Optional[str] = Field(
      None,
      description="name of the instance the metrics applies to if not an aggregate",
  )
  data: Dict[int, float] = Field(..., description="map of start times to metric values")


class MetricsQueryResponse(BaseModel): # pylint: disable=too-few-public-methods
  """A metrics timeline over an interval."""
  starttime: int = Field(..., alias="starttime")
  endtime: int = Field(..., alias="endtime")
  timeline: List[TimelinePoint] = Field(
      ..., description="list of timeline point objects",
  )

@router.get("/metrics/query", response_model=MetricsQueryResponse)
async def get_metrics_query( # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    query: str,
    role: Optional[str] = None,
    start_time: int = Query(..., alias="starttime"),
    end_time: int = Query(..., alias="endtime"),
    topology_name: str = Query(..., alias="topology"),
) -> MetricsQueryResponse:
  """
  '/metrics/query' 0.20.5 above.
  Run a metrics query against a particular topology.
  """
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  metrics = await TManagerQuery(state.tracker).execute_query(
      topology.tmanager, query, start_time, end_time
  )

  timeline = [
      TimelinePoint(data=metric.timeline, instance=metric.instance)
      for metric in metrics
  ]

  return MetricsQueryResponse(
      starttime=start_time,
      endtime=end_time,
      timeline=timeline,
  )
