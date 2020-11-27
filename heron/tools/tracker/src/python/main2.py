from typing import Generic, TypeVar, Dict, List, Literal, Optional

from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python import tracker
from heron.tools.tracker.src.python.routers import topologies, containers, metrics

from fastapi import FastAPI, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic.generics import GenericModel
from starlette.responses import (
    RedirectResponse,
    HTTPException as StarletteHTTPException,
)

app = FastAPI()
app.include_router(topologies.router, prefix="/topologies")

ResultType = TypeVar("ResultType")
# XXX: use of Pydantic generics requires Python 3.7+
class ResponseEnvelope(GenericModel, Generic[ResultType]):
    # XXX: looking to deprecate exception time - leve to logging or calling app
    executiontime: float = 0
    message: str
    result: Optional[ResultType] = None
    status: Literal[
        constants.RESPONSE_STATUS_FAILURE, constants.RESPONSE_STATUS_SUCCESS
    ]
    tracker_version: str = constants.API_VERSION


@app.exception_handler(Exception)
async def handle_exception(exc: Exception):
    payload = ResponseEnvelope(
        message=f"request failed: {exc}", status=constants.RESPONSE_STATUS_FAILURE
    )
    status_code = 500
    if isinstance(exc, StarletteHTTPException):
        status_code = exc.status_code
    if isinstance(exc, RequestValidationError):
        status_code = 400
    return JSONResponse(content=payload, status_code=status_code)


@app.get("/")
async def home():
    return RedirectResponse(url="/topologies")


@app.get("/clusters", response_model=ResponseEnvelope[List[str]])
async def clusters() -> List[str]:
    return [s.name for s in tracker.state_managers]


@app.get(
    "/machines",
    response_model=ResponseEnvelope[Dict[str, Dict[str, Dict[str, List[str]]]]],
)
async def get(
    cluster_names: List[str] = Query(..., alias="cluster"),
    environ_names: List[str] = Query(..., alias="environ"),
    topology_names: List[str] = Query(..., alias="topology"),
):
    """
    Return a map of topology (cluster, environ, name) to a list of machines found in the
    physical plans plans of maching topologies.

    If no names are provided, then all topologies matching the other filters are returned.

    """
    # XXX: test this - assuming that the list can be empty and valid
    # if topology names then clusters and environs needed
    if topology_names and not (cluster_names and environ_names):
        raise ValueError(
            "If topology names are provided then cluster and environ names must be provided"
        )

    response: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
    for topology in tracker.topologies:
        cluster, environ, name = topology.cluster, topology.environ, topology.name
        if cluster_names and cluster not in cluster_names:
            continue
        if environ_names and environ not in environ_names:
            continue
        if topology_names and name not in topology_names:
            continue

        response.setdefault(cluster, {}).setdefault(environ, {})[
            name
        ] = topology.get_machines()

    return response
