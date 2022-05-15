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
This service uses the configured state manager to retrieve notifications about
running topologies, and uses data from that to communicate with topology managers
when prompted to.

"""
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from heron.tools.tracker.src.python import constants, state, query
from heron.tools.tracker.src.python.routers import topologies, container, metrics

openapi_tags = [
    {"name": "metrics", "description": query.__doc__},
    {"name": "container", "description": container.__doc__},
    {"name": "topologies", "description": topologies.__doc__},
]

app = FastAPI(
    title="Heron Tracker",
    redoc_url="/",
    description=__doc__,
    version=constants.API_VERSION,
    openapi_tags=openapi_tags,
    externalDocs={
        "description": "Heron home page",
        "url": "https://heron.incubator.apache.org/",
    },
    info={
        "license": {
            "name": "Apache 2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0.html"
        },
    },
    **{
        "x-logo": {
            "url": "https://heron.incubator.apache.org/img/HeronTextLogo-small.png",
            "href": "https://heron.incubator.apache.org/",
            "backgroundColor": "#263238",
        }
    },
)
app.include_router(container.router, prefix="/topologies", tags=["container"])
app.include_router(metrics.router, prefix="/topologies", tags=["metrics"])
app.include_router(topologies.router, prefix="/topologies", tags=["topologies"])

@app.middleware("http")
async def wrap_response(request: Request, call_next):
  start_time = time.time()
  response = await call_next(request)
  process_time = time.time() - start_time
  response.headers["x-process-time"] = str(process_time)
  return response

@app.on_event("startup")
async def startup_event():
  """Start recieving topology updates."""
  state.tracker.sync_topologies()

@app.on_event("shutdown")
async def shutdown_event():
  """Stop recieving topology updates."""
  state.tracker.stop_sync()

@app.exception_handler(Exception)
async def handle_exception(_, exc: Exception):
  message = f"request failed: {exc}"
  status_code = 500
  if isinstance(exc, StarletteHTTPException):
    status_code = exc.status_code
  if isinstance(exc, RequestValidationError):
    status_code = 400
  return JSONResponse(content=message, status_code=status_code)

@app.get("/clusters")
async def clusters() -> List[str]:
  return (s.name for s in state.tracker.state_managers)
@app.get(
    "/machines",
    response_model=Dict[str, Dict[str, Dict[str, List[str]]]],
)
async def get_machines(
    cluster_names: Optional[List[str]] = Query(None, alias="cluster"),
    environ_names: Optional[List[str]] = Query(None, alias="environ"),
    topology_names: Optional[List[str]] = Query(None, alias="topology"),
):
  """
  Return a map of topology (cluster, environ, name) to a list of machines found in the
  physical plans plans of maching topologies.

  If no names are provided, then all topologies matching the other filters are returned.

  """
  # if topology names then clusters and environs needed
  if topology_names and not (cluster_names and environ_names):
    raise ValueError(
        "If topology names are provided then cluster and environ names must be provided"
    )

  response: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
  for topology in state.tracker.filtered_topologies(cluster_names, environ_names, topology_names):
    response.setdefault(topology.cluster, {}).setdefault(topology.environ, {})[
        topology.name
    ] = topology.get_machines()

  return response

@app.get("/health", response_class=PlainTextResponse)
def healthcheck():
  return "ok"
