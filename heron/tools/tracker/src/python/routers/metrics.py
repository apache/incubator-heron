"""
Views on Heron metrics.

"""
from typing import Dict, List, Optional

from heron.common.src.python.utils.log import Log
from heron.proto import common_pb2
from heron.proto import tmanager_pb2
from heron.tools.tracker.src.python import metricstimeline, state
from heron.tools.tracker.src.python.main2 import ResponseEnvelope
from heron.tools.tracker.src.python.query import Query as TManagerQuery

import httpx

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

router = APIRouter()


async def get_component_metrics(
  tmanager,
  component: str,
  metric_names: List[str],
  instances: List[str],
  interval: int,
):
  """
  Return metrics from the Tmanager over the given interval. Metrics not included
  in `metric_names` will be truncated.

  """
  if not (tmanager and tmanager.host and tmanager.stats_port):
    raise Exception("No Tmanager found")

  metric_request = tmanager_pb2.MetricRequest()
  metric_request.component_name = component
  if instances:
    metric_request.instance_id.extend(instances)
  metric_request.metric.extend(metric_names)
  metric_request.interval = interval
  url = f"http://{tmanager.host}:{tmanager.port}/stats"
  async with httpx.AsyncClient() as client:
    response = await client.post(url, data=metric_request.SerializeToString())
  metric_response = tmanager_pb2.MetricResponse()
  metric_response.ParseFromString(response.body)

  if metric_response.status.status == common_pb2.NOTOK:
    if metric_response.status.HasField("message"):
      Log.warn(
        "Recieved response from Tmanager: %s", metric_response.status.message
      )

  result = {
    "interval": metric_response.interval,
    "component": component,
    "metrics": {},
  }
  for metric in metric_response.metric:
    instance = metric.instance_id
    for instance_metric in metric.metric:
      result["metrics"].setdefault(instance_metric.name, {})[
        instance
      ] = instance_metric.value
  return result


@router.get("/metrics")
async def get_metrics( # pylint: disable=too-many-arguments
  cluster: str,
  role: str,
  environ: str,
  component: str,
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


@router.get("/metricstimeline")
async def get_metrics_timeline( # pylint: disable=too-many-arguments
  cluster: str,
  role: str,
  environ: str,
  component: str,
  start_time: int,
  end_time: int,
  topology_name: str = Query(..., alias="topology"),
  metric_names: Optional[List[str]] = Query(None, alias="metricname"),
  instances: Optional[List[str]] = Query(None, alias="instance"),
):
  """Return metrics over the given interval."""
  if start_time > end_time:
    raise RequestError("start_time > end_time")
  topology = state.tracker.get_toplogy(cluster, role, environ, topology_name)
  return await metricstimeline.get_metrics_timeline(
    topology.tmanager, component, metric_names, instances, start_time, end_time
  )


class TimelinePoint(BaseModel): # pylint: disable=too-few-public-methods
  """A metric at discrete points in time."""
  instance: Optional[str] = Field(
    None,
    description="name of the instance the metrics applies to if not an aggregate",
  )
  data: Dict[int, int] = Field(..., description="map of start times to metric values")


class MetricsQueryResponse(BaseModel): # pylint: disable=too-few-public-methods
  """A metrics timeline over an interval."""
  start_time: int = Field(..., alias="starttime")
  end_time: int = Field(..., alias="endtime")
  timeline: List[TimelinePoint] = Field(
    ..., description="list of timeline point objects"
  )


@router.get("/metricsquery", response_model=MetricsQueryResponse)
async def get_metrics_query( # pylint: disable=too-many-arguments
  cluster: str,
  role: str,
  environ: str,
  query: str,
  start_time: int = Query(..., alias="starttime"),
  end_time: int = Query(..., alias="endtime"),
  topology_name: str = Query(..., alias="topology"),
) -> MetricsQueryResponse:
  """Run a metrics query against a particular toplogy."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  metrics = await TManagerQuery(state.tracker).execute_query(
    topology.tmanager, query, start_time, end_time
  )

  timeline = []
  for metric in metrics:
    point = {"data": metric.timeline}
    if metric.instance:
      point["instance"] = metric.instance
    timeline.append(point)

  return {
    "startime": start_time,
    "endtime": end_time,
    "timeline": timeline,
  }
