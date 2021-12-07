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
These methods provide information and data about the state of a running
topology, particularly data about heron containers.

"""
from typing import List, Optional

from heron.proto import common_pb2, tmanager_pb2
from heron.tools.tracker.src.python import state, utils
from heron.tools.tracker.src.python.utils import EnvelopingAPIRouter

import httpx

from fastapi import Query
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

router = EnvelopingAPIRouter()


@router.get("/containerfiledata")
async def get_container_file_slice(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    role: Optional[str],
    container: str,
    path: str,
    offset: int,
    length: int,
    topology_name: str = Query(..., alias="topology"),
):
  """
  Return a range of bytes for the given file wrapped in JSON.

  Usually used to retrieve a log file chunk.

  """
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  stmgr = state.tracker.pb2_to_api(topology)["physical_plan"]["stmgrs"][f"stmgr-{container}"]
  url = f"http://{stmgr['host']}:{stmgr['shell_port']}/filedata/{path}"
  params = {"offset": offset, "length": length}

  with httpx.AsyncClient() as client:
    response = await client.get(url, params=params)
    return response.json()


@router.get("/containerfiledownload", response_class=StreamingResponse)
async def get_container_file(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    role: Optional[str],
    container: str,
    path: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Return a given raw file."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  stmgr = state.tracker.pb2_to_api(topology)["physical_plan"]["stmgrs"][f"stmgr-{container}"]
  url = f"http://{stmgr['host']}:{stmgr['shell_port']}/download/{path}"

  _, _, filename = path.rpartition("/")
  with httpx.stream("GET", url) as response:
    return StreamingResponse(
        content=response.iter_bytes(),
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/containerfilestats")
async def get_container_file_listing(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    role: Optional[str],
    container: str,
    path: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Return the stats for a given directory."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  stmgr = state.tracker.pb2_to_api(topology)["physical_plan"]["stmgrs"][f"stmgr-{container}"]
  url = utils.make_shell_filestats_url(stmgr["host"], stmgr["shell_port"], path)
  with httpx.AsyncClient() as client:
    response = await client.get(url)
    return response.json()


@router.get("/runtimestate")
async def get_container_runtime_state(
    cluster: str,
    role: Optional[str],
    environ: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Return the runtime state."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  topology_info = topology.info
  tmanager = topology.tmanager

  # find out what is registed
  if not (tmanager and tmanager.host and tmanager.stats_port):
    raise ValueError("TManager not set yet")
  url = f"http://{tmanager.host}:{tmanager.stats_port}/stmgrsregistrationsummary"
  with httpx.AsyncClient() as client:
    response = await client.post(
        url,
        data=tmanager_pb2.StmgrsRegistrationSummaryRequest().SerializeToString(),
    )
  response.raise_for_status()
  reg = tmanager_pb2.StmgrsRegistrationSummaryResponse()
  reg.ParseFromString(response.content)

  # update the result with registration status
  runtime_state = topology_info.runtime_state.copy()
  for stmgr, is_registered in (
      (reg.registered_stmgrs, True),
      (reg.absent_stmgrs, False),
  ):
    runtime_state.stmgrs[stmgr] = {"is_registered": is_registered}

  return runtime_state

class ExceptionLog(BaseModel):
  hostname: str
  instance_id: str
  stack_trace: str = Field(..., alias="stacktrace")
  last_time: int = Field(..., alias="lasttime")
  first_time: int = Field(..., alias="firsttime")
  count: str = Field(..., description="number of occurances during collection interval")
  logging: str = Field(..., description="additional text logged with exception")

async def _get_exception_log_response(
    cluster: str,
    role: Optional[str],
    environ: str,
    component: str,
    instances: List[str] = Query(..., alias="instance"),
    topology_name: str = Query(..., alias="topology"),
    summary: bool = False,
) -> List[tmanager_pb2.ExceptionLogResponse]:
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  tmanager = topology.tmanager

  if not (tmanager and tmanager.host and tmanager.stats_port):
    raise ValueError("TManager not set yet")
  exception_request = tmanager_pb2.ExceptionLogRequest()
  exception_request.component_name = component
  exception_request.instances.extend(instances)
  url_suffix = "ummary" if summary else ""
  url = f"http://{tmanager.host}:{tmanager.stats_port}/exceptions{url_suffix}"
  with httpx.AsyncClient() as client:
    response = await client.post(url, data=exception_request.SerializeToString())
  response.raise_for_status()

  exception_response = tmanager_pb2.ExceptionLogResponse()
  exception_response.ParseFromString(response.content)

  if exception_response.status.status == common_pb2.NOTOK:
    raise RuntimeError(
        exception_response.status.message
        if exception_response.status.HasField("message")
        else "an error occurred"
    )
  return exception_response


@router.get("/exceptions", response_model=List[ExceptionLog])
async def get_exceptions(  # pylint: disable=too-many-arguments
    cluster: str,
    role: Optional[str],
    environ: str,
    component: str,
    instances: List[str] = Query(..., alias="instance"),
    topology_name: str = Query(..., alias="topology"),
):
  """Return info about exceptions that have occurred per instance."""
  exception_response = await _get_exception_log_response(
      cluster, role, environ, component, instances, topology_name, summary=False
  )

  return [
      ExceptionLog(
          hostname=exception_log.hostname,
          instance_id=exception_log.instance_id,
          stack_trace=exception_log.stacktrace,
          lasttime=exception_log.lasttime,
          firsttime=exception_log.firsttime,
          count=str(exception_log.count),
          logging=exception_log.logging,
      )
      for exception_log in exception_response.exceptions
  ]


class ExceptionSummaryItem(BaseModel):
  class_name: str
  last_time: int = Field(..., alias="lasttime")
  first_time: int = Field(..., alias="firsttime")
  count: str

@router.get("/exceptionsummary", response_model=List[ExceptionSummaryItem])
async def get_exceptions_summary(  # pylint: disable=too-many-arguments
    cluster: str,
    role: Optional[str],
    environ: str,
    component: str,
    instances: List[str] = Query(..., alias="instance"),
    topology_name: str = Query(..., alias="topology"),
):
  """Return info about exceptions that have occurred."""
  exception_response = await _get_exception_log_response(
      cluster, role, environ, component, instances, topology_name, summary=False
  )

  return [
      ExceptionSummaryItem(
          class_name=exception_log.stacktrace,
          last_time=exception_log.lasttime,
          first_time=exception_log.firsttime,
          count=str(exception_log.count),
      )
      for exception_log in exception_response.exceptions
  ]


class ShellResponse(BaseModel):  # pylint: disable=too-few-public-methods
  """Response from heron-shell when executing remote commands."""

  command: str = Field(..., description="full command executed at server")
  stdout: str = Field(..., description="text on stdout")
  stderr: Optional[str] = Field(None, description="text on stderr")


@router.get("/pid", response_model=ShellResponse)
async def get_container_heron_pid(
    cluster: str,
    role: Optional[str],
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Get the PId of the heron process."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)
  base_url = utils.make_shell_endpoint(state.tracker.pb2_to_api(topology), instance)
  url = f"{base_url}/pid/{instance}"
  with httpx.AsyncClient() as client:
    return await client.get(url).json()


@router.get("/jstack", response_model=ShellResponse)
async def get_container_heron_jstack(
    cluster: str,
    role: Optional[str],
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Get jstack output for the heron process."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)

  pid_response = await get_container_heron_pid(cluster, role, environ, instance, topology_name)
  pid = pid_response["stdout"].strip()

  base_url = utils.make_shell_endpoint(state.tracker.pb2_to_api(topology), instance)
  url = f"{base_url}/jstack/{pid}"
  with httpx.AsyncClient() as client:
    return await client.get(url).json()


@router.get("/jmap", response_model=ShellResponse)
async def get_container_heron_jmap(
    cluster: str,
    role: Optional[str],
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Get jmap output for the heron process."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)

  pid_response = await get_container_heron_pid(cluster, role, environ, instance, topology_name)
  pid = pid_response["stdout"].strip()

  base_url = utils.make_shell_endpoint(state.tracker.pb2_to_api(topology), instance)
  url = f"{base_url}/jmap/{pid}"
  with httpx.AsyncClient() as client:
    return await client.get(url).json()


@router.get("/histo", response_model=ShellResponse)
async def get_container_heron_memory_histogram(
    cluster: str,
    role: Optional[str],
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
  """Get memory usage histogram the heron process. This uses the ouput of the last jmap run."""
  topology = state.tracker.get_topology(cluster, role, environ, topology_name)

  pid_response = await get_container_heron_pid(cluster, role, environ, instance, topology_name)
  pid = pid_response["stdout"].strip()

  base_url = utils.make_shell_endpoint(state.tracker.pb2_to_api(topology), instance)
  url = f"{base_url}/histo/{pid}"
  with httpx.AsyncClient() as client:
    return await client.get(url).json()
