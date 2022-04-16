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
'''
This module provides a synchronous client library for a tracker instance.

To use this client, you must provide the tracker_url, which is done with:
  heron.tools.common.src.python.clients.tracker.tracker_url = tracker_url
this is a reminent of the old tornado implementation and should be factored
out in some way.

This module isn't just a thin client for a tracker, it also includes
methods common to heron-explorer and heron-ui.

'''

import re
import time

from typing import Any, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from heron.common.src.python.utils.log import Log

# This requires setting
tracker_url = "http://127.0.0.1:8888"

CLUSTER_URL_FMT             = "%s/clusters"

# Nested under /topologies
TOPOLOGIES_URL_FMT          = "%s/topologies"
TOPOLOGIES_STATS_URL_FMT    = f"{TOPOLOGIES_URL_FMT}/states"
EXECUTION_STATE_URL_FMT     = f"{TOPOLOGIES_URL_FMT}/executionstate"
LOGICALPLAN_URL_FMT         = f"{TOPOLOGIES_URL_FMT}/logicalplan"
PHYSICALPLAN_URL_FMT        = f"{TOPOLOGIES_URL_FMT}/physicalplan"
PACKINGPLAN_URL_FMT         = f"{TOPOLOGIES_URL_FMT}/packingplan"
SCHEDULER_LOCATION_URL_FMT  = f"{TOPOLOGIES_URL_FMT}/schedulerlocation"

EXCEPTIONS_URL_FMT          = f"{TOPOLOGIES_URL_FMT}/exceptions"
EXCEPTION_SUMMARY_URL_FMT   = f"{TOPOLOGIES_URL_FMT}/exceptionsummary"

INFO_URL_FMT                = f"{TOPOLOGIES_URL_FMT}/info"
PID_URL_FMT                 = f"{TOPOLOGIES_URL_FMT}/pid"
JSTACK_URL_FMT              = f"{TOPOLOGIES_URL_FMT}/jstack"
JMAP_URL_FMT                = f"{TOPOLOGIES_URL_FMT}/jmap"
HISTOGRAM_URL_FMT           = f"{TOPOLOGIES_URL_FMT}/histo"

# nested under /topologies/metrics/
METRICS_URL_FMT             = f"{TOPOLOGIES_URL_FMT}/metrics"
METRICS_QUERY_URL_FMT       = f"{METRICS_URL_FMT}/query"
METRICS_TIMELINE_URL_FMT    = f"{METRICS_URL_FMT}/timeline"

# nested under /topologies/container/
CONTAINER_URL_FMT           = f"{TOPOLOGIES_URL_FMT}/container"
FILE_DATA_URL_FMT           = f"{CONTAINER_URL_FMT}/filedata"
FILE_DOWNLOAD_URL_FMT       = f"{CONTAINER_URL_FMT}/filedownload"
FILESTATS_URL_FMT           = f"{CONTAINER_URL_FMT}/filestats"


def strip_whitespace(s):
  return re.sub(r'\s','', s)

capacity = strip_whitespace("""
DIVIDE(
  DEFAULT(0,
    MULTIPLY(
      TS({0},{1},__execute-count/default),
      TS({0},{1},__execute-latency/default)
    )
  ),
  60000000000
)
""")

failures = strip_whitespace("""
DEFAULT(0,
  DIVIDE(
    TS({0},{1},__fail-count/default),
    SUM(
      DEFAULT(1, TS({0},{1},__execute-count/default)),
      DEFAULT(0, TS({0},{1},__fail-count/default))
    )
  )
)
""")

cpu = strip_whitespace("DEFAULT(0, TS({0},{1},__jvm-process-cpu-load))")

memory = strip_whitespace("""
DIVIDE(
  DEFAULT(0, TS({0},{1},__jvm-memory-used-mb)),
  DEFAULT(1, TS({0},{1},__jvm-memory-mb-total))
)
""")

gc = "RATE(TS({0},{1},__jvm-gc-collection-time-ms))"

backpressure = strip_whitespace("""
DEFAULT(0, TS(__stmgr__,*,__time_spent_back_pressure_by_compid/{0}))
""")

queries = dict(
    cpu=cpu,
    capacity=capacity,
    failures=failures,
    memory=memory,
    gc=gc,
    backpressure=backpressure
)

def api_get(url: str, params=None) -> Any:
  """Make a GET request to a tracker URL and return the result."""
  start = time.time()
  try:
    Log.debug(f"Requesting URL: {url} with params: {params}")
    response = requests.get(url, params)
    response.raise_for_status()
  except Exception as e:
    Log.error(f"Unable to get response from {url} with params {params}: {e}")
    return None
  end = time.time()
  data = response.json()
  if response.status_code != requests.codes.ok:
    Log.error("error from tracker: %s", response.status_code)
    return None

  execution = float(response.headers.get("x-process-time")) * 1000
  duration = (end - start) * 1000
  Log.debug(f"URL fetch took {execution:.2} ms server time for {url}")
  Log.debug(f"URL fetch took {duration:.2} ms round trip time for {url}")

  return data


def create_url(fmt: str) -> str:
  """Given an URL format, substitute with tracker service endpoint."""
  return fmt % tracker_url


def get_clusters() -> List[str]:
  """Return a list of cluster names."""
  request_url = create_url(CLUSTER_URL_FMT)
  return api_get(request_url)


def get_topologies_states() -> Any:
  """Get the list of topologies and their states."""
  request_url = create_url(TOPOLOGIES_STATS_URL_FMT)
  return api_get(request_url)


def get_topologies(
    cluster: Optional[str]=None,
    role: Optional[str]=None,
    env: Optional[str]=None,
) -> Any:
  """Get the list of topologies."""
  base_url = create_url(TOPOLOGIES_URL_FMT)
  params = {"cluster": cluster, "environ": env, "role": role}
  return api_get(base_url, params)


def get_execution_state(cluster: str, environ: str, topology: str, role: Optional[str]=None) -> Any:
  """Get the execution state of a topology in a cluster."""
  base_url = create_url(EXECUTION_STATE_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  return api_get(base_url, params)


def get_logical_plan(cluster: str, environ: str, topology: str, role: Optional[str]=None) -> Any:
  """Get the logical plan state of a topology in a cluster."""
  base_url = create_url(LOGICALPLAN_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  return api_get(base_url, params)


def get_comps(cluster: str, environ: str, topology: str, role: Optional[str]=None) -> Any:
  """Get the list of component names for the topology from Heron Nest."""
  base_url = create_url(LOGICALPLAN_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  lplan = api_get(base_url, params)
  return sorted(lplan["spouts"].keys() | lplan["bolts"].keys())

def get_instances(cluster: str, environ: str, topology: str, role: Optional[str]=None):
  """Get the list of instances for the topology from Heron Nest."""
  base_url = create_url(PHYSICALPLAN_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  pplan = api_get(base_url, params)
  return sorted(pplan['instances'])


def get_packing_plan(cluster: str, environ: str, topology: str, role: Optional[str]=None) -> Any:
  """Get the packing plan state of a topology in a cluster from tracker."""
  base_url = create_url(PACKINGPLAN_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  return api_get(base_url, params)


def get_physical_plan(cluster: str, environ: str, topology: str, role: Optional[str]=None) -> Any:
  """Get the physical plan state of a topology in a cluster from tracker."""
  base_url = create_url(PHYSICALPLAN_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  return api_get(base_url, params)


def get_scheduler_location(
    cluster: str,
    environ: str,
    topology: str,
    role: Optional[str]=None,
) -> Any:
  """Get the scheduler location of a topology in a cluster from tracker."""
  base_url = create_url(SCHEDULER_LOCATION_URL_FMT)
  params = {"cluster": cluster, "environ": environ, "topology": topology, "role": role}
  return api_get(base_url, params)


def get_component_exceptionsummary(
    cluster: str,
    environ: str,
    topology: str,
    component: str,
    role: Optional[str]=None,
) -> Any:
  """Get summary of exception for a component."""
  base_url = create_url(EXCEPTION_SUMMARY_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "component": component,
  }
  return api_get(base_url, params)


def get_component_exceptions(
    cluster: str,
    environ: str,
    topology: str,
    component: str,
    role: Optional[str]=None,
) -> Any:
  """Get exceptions for 'component' for 'topology'."""
  base_url = create_url(EXCEPTIONS_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "component": component,
  }
  return api_get(base_url, params)


def get_comp_instance_metrics(
    cluster: str,
    environ: str,
    topology: str,
    component: str,
    metrics: List[str],
    instances: List[str],
    time_range: Tuple[int, int],
    role: Optional[str]=None,
) -> Any:
  """
  Get the metrics for some instances of a topology from tracker
  :param metrics:     dict of display name to cuckoo name
  :param time_range:  2-tuple consisting of start and end of range

  """
  base_url = create_url(METRICS_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "component": component,
      "interval": time_range[1],
      "metricname": [m[0] for m in metrics.values()],
      "instance": instances if isinstance(instances, list) else [instances],
  }
  return api_get(base_url, params)


def get_comp_metrics(
    cluster: str,
    environ: str,
    topology: str,
    component: str,
    instances: List[str],
    metrics: List[str],
    time_range: Tuple[int, int],
    role: Optional[str]=None,
) -> Any:
  """
  Get the metrics for all the instances of a topology from Heron Nest
  :param metrics:       list of names, or ["*"] for all
  :param time_range:    2-tuple consisting of start and end of range
  """
  base_url = create_url(METRICS_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "component": component,
      "metricname": metrics,
      "instance": instances,
      "interval": time_range[1],
  }
  return api_get(base_url, params)


def get_metrics(
    cluster: str,
    environment: str,
    topology: str,
    timerange: str,
    query: str,
    role: Optional[str]=None,
) -> Any:
  """Get the metrics for a topology from tracker."""
  base_url = create_url(METRICS_QUERY_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environment,
      "role": role,
      "topology": topology,
      "starttime": timerange[0],
      "endtime": timerange[1],
      "query": query,
  }
  return api_get(base_url, params)


def get_comp_metrics_timeline(
    cluster: str,
    environ: str,
    topology: str,
    component: str,
    instances: List[str],
    metricnames: List[str],
    time_range: Tuple[int, int],
    role: Optional[str]=None,
) -> Any:
  """
  Get the minute-by-minute metrics for all instances of a topology from tracker
  :param metricnames:   dict of display name to cuckoo name
  :param time_range:    2-tuple consisting of start and end of range
  """
  base_url = create_url(METRICS_TIMELINE_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "role": role,
      "topology": topology,
      "component": component,
      "instance": instances,
      "metricname": metricnames,
      "starttime": time_range[0],
      "endtime": time_range[1],
  }
  return api_get(base_url, params)


def get_topology_info(cluster: str, environ: str, topology: str, role: Optional[str]=None) -> Any:
  """Return a dictionary containing information about a topology."""
  base_url = create_url(INFO_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
  }
  return api_get(base_url, params)


def get_instance_pid(
    cluster: str,
    environ: str,
    topology: str,
    instance: str,
    role: Optional[str]=None,
) -> Any:
  """Return instance pid."""
  base_url = create_url(PID_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "instance": instance,
  }
  return api_get(base_url, params)


def get_instance_jstack(
    cluster: str,
    environ: str,
    topology: str,
    instance: str,
    role: Optional[str]=None,
) -> None:
  """Call jstack on the given instance."""
  base_url = create_url(JSTACK_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "instance": instance,
  }
  return api_get(base_url, params)


def get_instance_mem_histogram(
    cluster: str,
    environ: str,
    topology: str,
    instance: str,
    role: Optional[str]=None,
) -> Any:
  """Get histogram of active memory objects."""
  base_url = create_url(HISTOGRAM_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "instance": instance,
  }
  return api_get(base_url, params)


def run_instance_jmap(cluster: str, environ: str, topology: str, instance: str, role=None) -> Any:
  """Call heap dump for an instance and save it at /tmp/heap.bin."""
  base_url = create_url(JMAP_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "instance": instance,
  }
  return api_get(base_url, params)


def get_container_file_download_url(
    cluster: str,
    environ: str,
    topology: str,
    container: str,
    path: str,
    role: Optional[str]=None,
) -> Any:
  """Return a URL for downloading a file from a container."""
  base_url = create_url(FILE_DOWNLOAD_URL_FMT)
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      container=container,
      path=path)
  if role is not None:
    params['role'] = role

  query = urlencode(params)
  return f"{base_url}?{query}"


def get_container_file_data(
    cluster: str,
    environ: str,
    topology: str,
    container: str,
    path: str,
    offset: int,
    length: int,
    role: Optional[str]=None,
) -> Any:
  """Return a range of data from a file (usually logs)."""
  base_url = create_url(FILE_DATA_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "container": container,
      "path": path,
      "offset": offset,
      "length": length,
  }
  return api_get(base_url, params)


def get_filestats(
    cluster: str,
    environ: str,
    topology: str,
    container: str,
    path: str,
    role: Optional[str]=None,
) -> Any:
  """Return a directory listing."""
  base_url = create_url(FILESTATS_URL_FMT)
  params = {
      "cluster": cluster,
      "environ": environ,
      "topology": topology,
      "role": role,
      "container": container,
      "path": path,
  }
  return api_get(base_url, params)


class HeronQueryHandler:
  ''' HeronQueryHandler '''


  def fetch(
      self,
      cluster: str,
      metric: str,
      topology: str,
      component: str,
      instance:str,
      timerange: Tuple[int, int],
      environ: Optional[str]=None,
  ) -> Any:
    """Fetch metrics."""
    components = [component] if component != "*" else get_comps(cluster, environ, topology)

    timelines = []
    for comp in components:
      query = self.get_query(metric, comp, instance)
      result = get_metrics(cluster, environ, topology, timerange, query)
      timelines.extend(result["timeline"])

    return self.get_metric_response(timerange, timelines, False)

  def fetch_max(
      self,
      cluster: str,
      metric: str,
      topology: str,
      component: str,
      instance: str,
      timerange: Tuple[int, int],
      environ: Optional[str]=None,
  ) -> Any:
    """Fetch max metrics."""
    components = [component] if component != "*" else get_comps(cluster, environ, topology)

    comp_metrics = []
    for comp in components:
      query = self.get_query(metric, comp, instance)
      max_query = f"MAX({query})"
      comp_metrics.append(get_metrics(cluster, environ, topology, timerange, max_query))

    data = self.compute_max(comp_metrics)

    return self.get_metric_response(timerange, data, True)

  # pylint: disable=unused-argument
  def fetch_backpressure(
      self,
      cluster: str,
      metric: str,
      topology: str,
      component: str,
      instance: str,
      timerange: Tuple[int, int],
      is_max: bool,
      environ: Optional[str]=None,
  ) -> Any:
    """Fetch backpressure."""
    instances = get_instances(cluster, environ, topology)
    if component != "*":
      filtered_inst = [instance for instance in instances if instance.split("_")[2] == component]
    else:
      filtered_inst = instances

    inst_metrics = {}
    for inst in filtered_inst:
      query = queries[metric].format(inst)
      inst_metrics[inst] = get_metrics(cluster, environ, topology, timerange, query)


    if is_max:
      data = self.compute_max(inst_metrics.values())
      return self.get_metric_response(timerange, data, is_max)

    timelines = []
    for i, metrics in inst_metrics.items():
      # Replacing stream manager instance name with component instance name
      if len(metrics["timeline"]) > 0:
        metrics["timeline"][0]["instance"] = i
      timelines.extend(metrics["timeline"])
    return self.get_metric_response(timerange, timelines, is_max)

  # pylint: disable=no-self-use
  def compute_max(self, multi_ts: Iterable[dict]) -> dict:
    """Return the max for a list of timeseries."""
    # Some components don't have specific metrics such as capacity hence the
    # key set is empty. These components are filtered out first.
    filtered_ts = [ts for ts in multi_ts if len(ts["timeline"][0]["data"]) > 0]
    if len(filtered_ts) > 0 and len(filtered_ts[0]["timeline"]) > 0:
      keys = list(filtered_ts[0]["timeline"][0]["data"].keys())
      timelines = ([res["timeline"][0]["data"][key] for key in keys] for res in filtered_ts)
      values = (max(v) for v in zip(*timelines))
      return dict(zip(keys, values))
    return {}

  # pylint: disable=no-self-use
  def get_metric_response(self, timerange: Tuple[int, int], data: dict, is_max: bool) -> dict:
    """Return a query response in the usual envelope."""
    if is_max:
      return dict(
          status="success",
          starttime=timerange[0],
          endtime=timerange[1],
          result=dict(timeline=[dict(data=data)])
      )

    return dict(
        status="success",
        starttime=timerange[0],
        endtime=timerange[1],
        result=dict(timeline=data)
    )

  # pylint: disable=no-self-use
  def get_query(self, metric: str, component: str, instance: str) -> str:
    """Return a formatted query."""
    return queries[metric].format(component, instance)


def _all_metric_queries() -> Tuple[List[str], List[str], List[str], List[str]]:
  # the count queries look suspicious, as if they should be parameterised with the environment
  normal_query_labels = [
      'complete-latency',
      'execute-latency',
      'process-latency',
      'jvm-uptime-secs',
      'jvm-process-cpu-load',
      'jvm-memory-used-mb']
  normal_queries = [f'__{m}' for m in normal_query_labels]
  count_query_labels = ['emit-count', 'execute-count', 'ack-count', 'fail-count']
  count_queries = [f'__{m}/default' for m in count_query_labels]
  return normal_queries, normal_query_labels, count_queries, count_query_labels


def metric_queries() -> List[str]:
  """all metric queries."""
  normal_queries, _, count_queries, _ = _all_metric_queries()
  return normal_queries + count_queries


def queries_map() -> dict:
  """map from query parameter to query name."""
  normal_queries, normal_query_labels, count_queries, count_query_labels = _all_metric_queries()
  result = dict(zip(normal_queries, normal_query_labels))
  result.update(zip(count_queries, count_query_labels))
  return result
