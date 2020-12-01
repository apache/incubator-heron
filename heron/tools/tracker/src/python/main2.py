"""
WSGI application for the tracker.

"""
from typing import Dict, List, Optional

from heron.tools.tracker.src.python import constants, state
from heron.tools.tracker.src.python.utils import ResponseEnvelope
from heron.tools.tracker.src.python.routers import topologies, container, metrics

from fastapi import FastAPI, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

# TODO: implement a 120s timeout to be consistent with previous implementation
app = FastAPI(redoc_url="/")
app.include_router(container.router, prefix="/topologies")
app.include_router(metrics.router, prefix="/topologies")
app.include_router(topologies.router, prefix="/topologies")


#APIRouter.api_route can be patched to take response_model, then add ResponseEnvelope to it
# and decorate view so that it returns success ResponseEnvelope. This approach is fine if
# adding routes to individual router, rather than directly to app.

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
  payload = ResponseEnvelope[str](
      result="",
      execution_time=0.0,
      message=f"request failed: {exc}",
      status=constants.RESPONSE_STATUS_FAILURE
  )
  status_code = 500
  if isinstance(exc, StarletteHTTPException):
    status_code = exc.status_code
  if isinstance(exc, RequestValidationError):
    status_code = 400
  return JSONResponse(content=payload.dict(), status_code=status_code)


@app.get("/clusters", response_model=ResponseEnvelope[List[str]])
async def clusters() -> List[str]:
  return ResponseEnvelope[List[str]](
      execution_time=0.0,
      message="ok",
      status="success",
      result=[s.name for s in state.tracker.state_managers],
  )

@app.get(
    "/machines",
    response_model=ResponseEnvelope[Dict[str, Dict[str, Dict[str, List[str]]]]],
)
async def get(
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

  return ResponseEnvelope[Dict[str, Dict[str, Dict[str, List[str]]]]](
      execution_time=0.0,
      result=response,
      status="success",
      message="ok",
  )
