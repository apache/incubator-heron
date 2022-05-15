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
heron-ui provides a web interface for exploring the state of a
cluster as reported by a tracker.

"""
import logging
import os.path
import sys
import time

from collections import Counter
from datetime import datetime
from typing import Callable, List, Optional

import click
import pydantic
import requests
import uvicorn

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import RedirectResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from heron.tools.common.src.python.utils import config
from heron.tools.common.src.python.clients import tracker
from heron.common.src.python.utils import log


VERSION = config.get_version_number()
DEFAULT_ADDRESS = "0.0.0.0"
DEFAULT_PORT = 8889
DEFAULT_TRACKER_URL = "http://127.0.0.1:8888"
DEFAULT_BASE_URL = ""

base_url = DEFAULT_BASE_URL
tracker_url = DEFAULT_TRACKER_URL

Log = log.Log
Log.setLevel(logging.DEBUG)

app = FastAPI(title="Heron UI", version=VERSION)

templates = Jinja2Templates(
    directory=os.path.join(sys.path[0], "heron/tools/ui/resources/templates")
)
topologies_router = APIRouter()


@app.get("/")
def home():
  """Redirect from root to topologies listing."""
  return RedirectResponse(url=app.url_path_for("topologies_page"))


@topologies_router.get("")
def topologies_page(request: Request) -> Response:
  """Return a rendered list of topologies."""
  return templates.TemplateResponse("topologies.html", {
      "topologies": [],
      "clusters": [str(cluster) for cluster in tracker.get_clusters()],
      "active": "topologies",
      "baseUrl": base_url,
      "request": request,
  })


@topologies_router.get("/{cluster}/{environment}/{topology}/config")
def config_page(
    cluster: str,
    environment: str,
    topology: str,
    request: Request,
) -> Response:
  """Render a HTML page of config for a topology."""
  return templates.TemplateResponse(
      "config.html",
      {
          "cluster": cluster,
          "environ": environment,
          "topology": topology,
          "active": "topologies",
          "baseUrl": base_url,
          "request": request,
      },
  )


@topologies_router.get("/{cluster}/{environment}/{topology}/{component}/{instance}/exceptions")
def exceptions_page(
    cluster: str, environment: str, topology: str, component: str, instance: str,
    request: Request
) -> Response:
  """Render a HTML page of exceptions for a container."""
  return templates.TemplateResponse(
      "exception.html",
      {
          "cluster": cluster,
          "environ": environment,
          "topology": topology,
          "comp_name": component,
          "instance": instance,
          "active": "topologies",
          "baseUrl": base_url,
          "request": request,
      },
  )


@topologies_router.get("/{cluster}/{environment}/{topology}")
def planner_page(
    cluster: str,
    environment: str,
    topology: str,
    request: Request,
) -> Response:
  """Render a HTML page to show information about a topology."""
  execution_state = tracker.get_execution_state(cluster, environment, topology)
  scheduler_location = tracker.get_scheduler_location(
      cluster, environment, topology
  )
  # is the tracker really making links for the UI!?
  job_page_link = scheduler_location["job_page_link"]
  launched_at = datetime.utcfromtimestamp(execution_state["submission_time"])
  launched_time = launched_at.isoformat(" ") + "Z"

  return templates.TemplateResponse(
      "topology.html",
      {
          "cluster": cluster,
          "environ": environment,
          "topology": topology,
          "execution_state": execution_state,
          "launched": launched_time,
          "status": "unknown",  # supposed to be "running" or "errors", but not implemented
          "active": "topologies",
          "job_page_link": job_page_link,
          "baseUrl": base_url,
          "request": request,
      },
  )


@topologies_router.get("/metrics")
def metrics(
    cluster: str,
    environ: str,
    topology: str,
    metric_names: List[str] = Query(None, alias="metricname"),
    instances: List[str] = Query(None, alias="instance"),
    component: Optional[str] = None,
    interval: int = -1,
) -> dict:
  """Return metrics for a given time range."""
  time_range = (0, interval)
  component_names = (
      [component]
      if component else
      tracker.get_comps(cluster, environ, topology)
  )
  # could make this async
  result = {
      # need to port over everything from access to tracker
      c: tracker.get_comp_metrics(
          cluster, environ, topology, c, instances, metric_names, time_range)
      for c in component_names
  }
  # switching the payload shape is bad, so this should be factored out in the future
  if component:
    return result[component]
  return result

query_handler = tracker.HeronQueryHandler()
@topologies_router.get("/metrics/timeline")
def timeline(
    cluster: str,
    environ: str,
    topology: str,
    metric: str,
    instance: str,
    starttime: int,
    endtime: int,
    component: Optional[str] = None,
    max: bool = False, # pylint: disable=redefined-builtin
) -> dict:
  """Return metrics for a given time range."""
  timerange = (starttime, endtime)
  component_names = (
      [component]
      if component else
      tracker.get_comps(cluster, environ, topology)
  )
  if metric == "backpressure":
    result = {
        c: query_handler.fetch_backpressure(
            cluster, metric, topology, c,
            instance, timerange, max, environ,
        )
        for c in component_names
    }
  else:
    fetch = query_handler.fetch_max if max else query_handler.fetch
    result = {
        c: fetch(cluster, metric, topology, c, instance, timerange, environ)
        for c in component_names
    }
  # switching the payload shape is bad, so this should be factored out in the future
  if component:
    return result[component]
  return result


@topologies_router.get("/filestats/{cluster}/{environment}/{topology}/{container}/file")
def file_stats_page(
    cluster: str, environment: str, topology: str,
    container: str, request: Request, path: str = ".",
) -> Response:
  """Render a HTML page for exploring a container's files."""
  data = tracker.get_filestats(cluster, environment, topology, container, path)
  return templates.TemplateResponse(
      "browse.html",
      {
          "cluster": cluster,
          "environ": environment,
          "topology": topology,
          "container": container,
          "path": path,
          "filestats": data,
          "baseUrl": base_url,
          "request": request,
      },
  )


@topologies_router.get("/{cluster}/{environment}/{topology}/{container}/file")
def file_page(
    cluster: str, environment: str, topology: str, container: str, path: str,
    request: Request,
) -> Response:
  """Render a HTML page for retrieving a container's file."""
  return templates.TemplateResponse(
      "file.html",
      {
          "cluster": cluster,
          "environ": environment,
          "topology": topology,
          "container": container,
          "path": path,
          "baseUrl": base_url,
          "request": request,
      },
  )


@topologies_router.get("/{cluster}/{environment}/{topology}/{container}/filedata")
def file_data(
    cluster: str,
    environment: str,
    topology: str,
    container: str,
    offset: int,
    length: int,
    path: str,
) -> Response:
  """Return a byte-range of data from a container's file."""
  # this should just use the byte-range header in a file download method
  data = tracker.get_container_file_data(
      cluster, environment, topology, container, path, offset, length
  )
  return data


@topologies_router.get("/{cluster}/{environment}/{topology}/{container}/filedownload")
def file_download(
    cluster: str, environment: str, topology: str, container: str, path: str
) -> Response:
  """Return a file from a container."""
  filename = os.path.basename(path)
  # make a streaming response and use a streaming download client
  download_url = tracker.get_container_file_download_url(
      cluster, environment, topology, container, path
  )
  data = requests.get(download_url)
  return Response(
      content=data.content,
      media_type="application/binary",
      headers={"Content-Disposition": f"attachment; filename={filename}"},
  )

# List envelope for Exceptions response
class ApiListEnvelope(pydantic.BaseModel):
  """Envelope for heron-ui JSON API."""
  status: str
  message: str
  version: str = VERSION
  executiontime: int
  result: list

def api_topology_list_json(method: Callable[[], dict]) -> ApiListEnvelope:
  """Wrap the output of a method with a response envelope."""
  started = time.time()
  result = method()
  Log.debug(f"Api topology: {result}")
  if result is None:
    return ApiEnvelope(
        status="failure",
        message="No topology found",
        executiontime=time.time() - started,
        result={},
    )
  return ApiListEnvelope(
      status="success",
      message="",
      executiontime=time.time() - started,
      result=result,
  )

# topology list and plan handlers
class ApiEnvelope(pydantic.BaseModel):
  """Envelope for heron-ui JSON API."""
  status: str
  message: str
  version: str = VERSION
  executiontime: int
  result: dict

def api_topology_json(method: Callable[[], dict]) -> ApiEnvelope:
  """Wrap the output of a method with a response envelope."""
  started = time.time()
  result = method()
  Log.debug(f"Api topology: {result}")
  if result is None:
    return ApiEnvelope(
        status="failure",
        message="No topology found",
        executiontime=time.time() - started,
        result={},
    )
  return ApiEnvelope(
      status="success",
      message="",
      executiontime=time.time() - started,
      result=result,
  )

@topologies_router.get("/list.json")
def topologies_json() -> dict:
  """Return the (mutated) list of topologies."""
  topologies = tracker.get_topologies_states()
  result = {}
  for c, cluster_value in topologies.items():
    result[c] = {}
    for e, environment_value in cluster_value.items():
      result[c][e] = {}
      for t, topology_value in environment_value.items():
        if topology_value.get("jobname") is None:
          continue
        # transforming payloads is usually an indicator of a bad shape
        topology_value.setdefault("submission_time", "-")
        result[c][e][t] = topology_value
  return result


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/logicalplan.json", response_model=ApiEnvelope
)
def logical_plan_json(cluster: str, environment: str, topology: str) -> ApiEnvelope:
  """Return the logical plan object for a topology."""
  return api_topology_json(lambda: tracker.get_logical_plan(
      cluster, environment, topology, None,
  ))


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/packingplan.json", response_model=ApiEnvelope
)
def packing_plan_json(cluster: str, environment: str, topology: str) -> ApiEnvelope:
  """Return the packing plan object for a topology."""
  return api_topology_json(lambda: tracker.get_packing_plan(
      cluster, environment, topology, None,
  ))


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/physicalplan.json", response_model=ApiEnvelope
)
def physical_plan_json(cluster: str, environment: str, topology: str) -> ApiEnvelope:
  """Return the physical plan object for a topology."""
  return api_topology_json(lambda: tracker.get_physical_plan(
      cluster, environment, topology, None,
  ))


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/executionstate.json", response_model=ApiEnvelope
)
def execution_state_json(cluster: str, environment: str, topology: str) -> ApiEnvelope:
  """Return the execution state object for a topology."""
  return api_topology_json(lambda: tracker.get_execution_state(
      cluster, environment, topology,
  ))


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/schedulerlocation.json",
    response_model=ApiEnvelope,
)
def scheduler_location_json(cluster: str, environment: str, topology: str) -> ApiEnvelope:
  """Unimplemented method which is currently a duplicate of execution state."""
  return api_topology_json(lambda: tracker.get_scheduler_location(
      cluster, environment, topology,
  ))


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/{component}/exceptions.json",
    response_model=ApiListEnvelope,
)
def exceptions_json(cluster: str, environment: str, topology: str,
                    component: str) -> ApiListEnvelope:
  """Return a list of exceptions for a component."""
  return api_topology_list_json(lambda: tracker.get_component_exceptions(
      cluster, environment, topology, component,
  ))

@topologies_router.get(
    "/{cluster}/{environment}/{topology}/{component}/exceptionsummary.json",
    response_model=ApiEnvelope,
)
def exception_summary_json(
    cluster: str, environment: str, topology: str, component: str
) -> ApiEnvelope:
  """Return a table of exception classes to totals."""
  started = time.time()
  if component.lower() == "all":
    logical_plan = tracker.get_logical_plan(cluster, environment, topology)
    if not logical_plan or not {"bolts", "spouts"} <= logical_plan.keys():
      return {}
    # looks like topologies can have spouts but no bolts, so they're assumed to be empty - should
    # the above key check be removed and replaced with bolts defaulting to an empty list?
    component_names = [*logical_plan["spouts"], *logical_plan["bolts"]]
  else:
    component_names = [component]

  exception_infos = {
      c: tracker.get_component_exceptionsummary(cluster, environment, topology, c)
      for c in component_names
  }

  class_counts = Counter()
  for exception_logs in exception_infos.values():
    for exception_log in exception_logs:
      class_counts[exception_log["class_name"]] += int(exception_log["count"])

  aggregate_exceptions_table = [
      [class_name, str(count)]
      for class_name, count in class_counts.items()
  ]

  return ApiEnvelope(
      status="success",
      message="",
      executiontime=time.time() - started,
      result=aggregate_exceptions_table,
  )


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/{instance}/pid"
)
def pid_snippet(
    request: Request,
    cluster: str,
    environment: str,
    topology: str,
    instance: str,
) -> Response:
  """Render a HTML snippet containing topology output of container."""
  physical_plan = tracker.get_physical_plan(cluster, environment, topology)
  host = physical_plan["stmgrs"][physical_plan["instances"][instance]["stmgr_id"]][
      "host"
  ]
  info = tracker.get_instance_pid(cluster, environment, topology, instance)
  command = info["command"]
  stdout = info["stdout"]
  return templates.TemplateResponse(
      "shell.snip.html",
      {
          "request": request,
          "host": host,
          "command": command,
          "output": stdout,
      },
  )



@topologies_router.get(
    "/{cluster}/{environment}/{topology}/{instance}/jstack"
)
def jstack_snippet(
    request: Request,
    cluster: str,
    environment: str,
    topology: str,
    instance: str,
) -> HTMLResponse:
  """Render a HTML snippet containing jstack output of container."""
  physical_plan = tracker.get_physical_plan(cluster, environment, topology)
  host = physical_plan["stmgrs"][physical_plan["instances"][instance]["stmgr_id"]][
      "host"
  ]
  info = tracker.get_instance_jstack(cluster, environment, topology, instance)
  command = info["command"]
  stdout = info["stdout"]
  return templates.TemplateResponse(
      "shell.snip.html",
      {
          "request": request,
          "host": host,
          "command": command,
          "output": stdout,
      },
  )


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/{instance}/jmap"
)
def jmap_snippet(
    request: Request,
    cluster: str,
    environment: str,
    topology: str,
    instance: str,
) -> HTMLResponse:
  """Render a HTML snippet containing jmap output of container."""
  physical_plan = tracker.get_physical_plan(cluster, environment, topology)
  host = physical_plan["stmgrs"][physical_plan["instances"][instance]["stmgr_id"]][
      "host"
  ]
  info = tracker.run_instance_jmap(cluster, environment, topology, instance)
  command = info["command"]
  stdout = info["stdout"]
  info = """
      <ul>
        <li>May take longer than usual (1-2 minutes) please be patient.</li>
        <li>Use SCP to copy heap dump files from host. (SCP {host}:/tmp/heap.bin /tmp/)</li>
      </ul>
  """
  return templates.TemplateResponse(
      "shell.snip.html",
      {
          "request": request,
          "host": host,
          "command": command,
          "output": stdout,
          "info": info,
      },
  )


@topologies_router.get(
    "/{cluster}/{environment}/{topology}/{instance}/histo"
)
def histogram_snippet(
    request: Request,
    cluster: str,
    environment: str,
    topology: str,
    instance: str,
) -> HTMLResponse:
  """Render a HTML snippet containing jmap histogram output of container."""
  # use a function to DRY up these container API methods
  physical_plan = tracker.get_physical_plan(cluster, environment, topology)
  host = physical_plan["stmgrs"][physical_plan["instances"][instance]["stmgr_id"]][
      "host"
  ]
  info = tracker.get_instance_mem_histogram(
      cluster, environment, topology, instance
  )
  command = info["command"]
  stdout = info["stdout"]
  return templates.TemplateResponse(
      "shell.snip.html",
      {
          "request": request,
          "host": host,
          "command": command,
          "output": stdout,
      },
  )


@app.get("/health", response_class=PlainTextResponse)
def healthcheck():
  return "ok"


app.include_router(topologies_router, prefix="/topologies")
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(sys.path[0], "heron/tools/ui/resources/static")),
    name="static",
)

@app.exception_handler(StarletteHTTPException)
async def unicorn_exception_handler(request: Request, exc: StarletteHTTPException) -> Response:
  if exc.status_code == 404:
    message = "URL not found"
  else:
    message = str(exc)
  return templates.TemplateResponse("error.html", {"errormessage": message, "request": request})

def show_version(_, __, value):
  if value:
    config.print_build_info()
    sys.exit(0)

@click.command()
@click.option("--tracker-url", "tracker_url_option", default=DEFAULT_TRACKER_URL)
@click.option("--base-url", "base_url_option", default=DEFAULT_BASE_URL)
@click.option("--host", default=DEFAULT_ADDRESS)
@click.option("--port", type=int, default=DEFAULT_PORT)
@click.option("--verbose", is_flag=True)
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=show_version,
)
def cli(
    host: str, port: int, base_url_option: str, tracker_url_option: str, verbose: bool
) -> None:
  """Start a web UI for heron which renders information from the tracker."""
  global base_url
  base_url = base_url_option
  log_level = logging.DEBUG if verbose else logging.INFO
  log.configure(log_level)

  tracker.tracker_url = tracker_url_option

  uvicorn.run(app, host=host, port=port, log_level=log_level)


if __name__ == "__main__":
  cli() # pylint: disable=no-value-for-parameter
