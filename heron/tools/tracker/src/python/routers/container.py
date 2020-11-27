"""
Views on Heron containers/instances within toplogies.

"""
from typing import List, Optional

from heron.proto import common_pb2, tmanager_pb2
from heron.tools.tracker.src.python import tracker
from heron.tools.tracker.src.python import utils

import httpx

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

router = APIRouter()


@router.get("/containerfiledata")
async def get_file_data(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    role: str,
    container: str,
    path: str,
    offset: int,
    length: int,
    topology_name: str = Query(..., "topology"),
):
    """
    Return a range of bytes for the given file wrapped in JSON.

    Usually used to retrieve a log file chunk.

    """
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)
    stmgr = topology["physical_plan"]["stmgrs"][f"stmgr-{container}"]
    url = f"http://{stmgr['host']}:{stmgr['shell_port']}/filedata/{path}"
    params = {"offset": offset, "length": length}

    with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        return response.json()


@router.get("/containerfiledownload")
async def get_file_download(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    role: str,
    container: str,
    path: str,
    topology_name: str = Query(..., "topology"),
):
    """Return the data for a given file."""
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)
    stmgr = topology["physical_plan"]["stmgrs"][f"stmgr-{container}"]
    url = f"http://{stmgr['host']}:{stmgr['shell_port']}/download/{path}"

    _, _, filename = path.rpartition("/")
    with httpx.stream("GET", url) as response:
        return StreamingResponse(
            content=response.iter_bytes(),
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )


@router.get("/containerfilestats")
async def get_file_stats(  # pylint: disable=too-many-arguments
    cluster: str,
    environ: str,
    role: str,
    container: str,
    path: str,
    topology_name: str = Query(..., "topology"),
):
    """Return the stats for a given directory."""
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)
    stmgr = topology["physical_plan"]["stmgrs"][f"stmgr-{container}"]
    url = utils.make_shell_filestats_url(stmgr["host"], stmgr["shell_port"], path)
    with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()


@router.get("/runtimestate")
async def get_runtime_state(
    cluster: str,
    role: str,
    environ: str,
    topology_name: str = Query(..., alias="topology"),
):
    """Return the runtime state."""
    topology_info = tracker.get_topology_info(topology_name, cluster, role, environ)
    topology = tracker.get_topology(cluster, role, environ, topology_name)
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
    state = topology_info["runtime_state"]
    # XXX: another mutation, looks bad - check it doesn't modify globals
    state["topology_version"] = topology_info["metadata"]["release_version"]
    for stmgr, is_registered in (
        (reg.registered_stmgrs, True),
        (reg.absent_stmgrs, False),
    ):
        state["stmgrs"].setdefault(stmgr, {})["is_registered"] = is_registered

    return state


async def _get_exception_log_response(
    cluster: str,
    role: str,
    environ: str,
    component: str,
    instances: List[str] = Query(..., alias="instance"),
    topology_name: str = Query(..., alias="topology"),
    summary: bool = False,
) -> tmanager_pb2.ExceptionLogResponse:
    topology = tracker.get_topology(cluster, role, environ, topology_name)
    tmanager = topology.tmanager

    if not (tmanager and tmanager.host and tmanager.stats_port):
        raise ValueError("TManager not set yet")
    exception_request = tmanager_pb2.ExceptionLogRequest()
    exception_request.component_name = component
    exception_request.instances.extend(instances)
    url_suffix = "ummary" if summary else ""
    url = f"http://{tmanager.host}:{tmanager.stats_port}/exceptions{suffix}"
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


@router.get("/exceptions")
async def get_exceptions(  # pylint: disable=too-many-arguments
    cluster: str,
    role: str,
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
        {
            "hostname": exception_log.hostname,
            "instance_id": exception_log.instance_id,
            # inconsistency
            "stack_trace": exception_log.stacktrace,
            "lasttime": exception_log.lasttime,
            "firsttime": exception_log.firsttime,
            # odd transformation
            "count": str(exception_log.count),
            "logging": exception_log.logging,
        }
        for exception_log in exception_response.exceptions
    ]


@router.get("/exceptionsummary")
async def get_exception_summary(  # pylint: disable=too-many-arguments
    cluster: str,
    role: str,
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
        {
            # inconsistency
            "class_name": exception_log.stacktrace,
            "lasttime": exception_log.lasttime,
            "firsttime": exception_log.firsttime,
            # odd transformation
            "count": str(exception_log.count),
        }
        for exception_log in exception_response.exceptions
    ]


class ShellResponse(BaseModel):  # pylint: disable=too-few-public-methods
    """Response from heron-shell when executing remote commands."""

    command: str = Field(..., description="full command executed at server")
    stdout: str = Field(..., description="text on stdout")
    stderr: Optional[str] = Field(None, description="text on stderr")


@router.get("/pid", response_model=ShellResponse)
async def get_pid(
    cluster: str,
    role: str,
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
    """Get the PId of the heron process."""
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)
    base_url = utils.make_shell_endpoint(topology, instance)
    url = f"{base_url}/pid/{instance}"
    with httpx.AsyncClient() as client:
        return await client.get(url).json()


@router.get("/jstack", response_model=ShellResponse)
async def get_jstack(
    cluster: str,
    role: str,
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
    """Get jstack output for the heron process."""
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)

    pid_response = await get_pid(cluster, role, environ, instance, topology_name)
    pid = pid_response["stdout"].strip()

    base_url = utils.make_shell_endpoint(topology, instance)
    url = f"{base_url}/jstack/{pid}"
    with httpx.AsyncClient() as client:
        return await client.get(url).json()


@router.get("/jmap", response_model=ShellResponse)
async def get_jmap(
    cluster: str,
    role: str,
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
    """Get jmap output for the heron process."""
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)

    pid_response = await get_pid(cluster, role, environ, instance, topology_name)
    pid = pid_response["stdout"].strip()

    base_url = utils.make_shell_endpoint(topology, instance)
    url = f"{base_url}/jmap/{pid}"
    with httpx.AsyncClient() as client:
        return await client.get(url).json()


@router.get("/histo", response_model=ShellResponse)
async def get_memory_histogram(
    cluster: str,
    role: str,
    environ: str,
    instance: str,
    topology_name: str = Query(..., alias="topology"),
):
    """Get memory usage histogram the heron process. This uses the ouput of the last jmap run."""
    topology = tracker.get_topology_info(topology_name, cluster, role, environ)

    pid_response = await get_pid(cluster, role, environ, instance, topology_name)
    pid = pid_response["stdout"].strip()

    base_url = utils.make_shell_endpoint(topology, instance)
    url = f"{base_url}/histo/{pid}"
    with httpx.AsyncClient() as client:
        return await client.get(url).json()
