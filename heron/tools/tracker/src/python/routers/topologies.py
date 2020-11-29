"""
Views on Heron toplogies.

"""
from typing import List, Optional, Dict

from heron.tools.tracker.src.python import state
from heron.tools.tracker.src.python.main2 import ResponseEnvelope

import networkx

from fastapi import APIRouter, Query

router = APIRouter()


@router.get("", response_model=ResponseEnvelope[Dict[str, Dict[str, List[str]]]])
async def get_topologies(
    role: Optional[str], # what is rolve vs. name?
    cluster_names: List[str] = Query(None, alias="cluster"),
    environ_names: List[str] = Query(None, alias="environ"),
):
  """
  Return a map of (cluster, role) to a list of topology names.

  """
  result = {}
  for topology in state.tracker.filtered_topologies(cluster_names, environ_names, roles={role}):
    if topology.execution_state:
      t_role = topology.execution_state.role
    else:
      t_role = None
    result.setdefault(topology.cluster, {}).setdefault(t_role, []).append(
        topology.name
    )
  return result


@router.get("/states")
async def get_states(
    cluster_names: Optional[List[str]] = Query(None, alias="cluster"),
    environ_names: Optional[List[str]] = Query(None, alias="environ"),
    role: Optional[str] = Query(None, description="ignored", deprecated=True),
):
  """Return the execution states for topologies. Keyed by (cluster, environ, topology)."""
  result = {}

  for topology in state.tracker.filtered_topologies(cluster_names, environ_names, {}, {role}):
    topology_info = state.tracker.pb2_to_api(topology)
    if topology_info is not None:
      result.setdefault(topology.cluster, {}).setdefault(topology.environ, {})[
          topology.name
      ] = topology_info["execution_state"]
  return result


@router.get("/info")
async def get_info(
    cluster: str, role: str, environ: str, topology: str,
):
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return state.tracker.pb2_to_api(topology)


# XXX: this all smells like graphql
@router.get("/config")
async def get_config(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  topology_info = state.tracker.pb2_to_api(topology)
  return topology_info["physical_plan"]["config"]


@router.get("/physicalplan")
async def get_physical_plan(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return state.tracker.pb2_to_api(topology)["physical_plan"]


# Deprecated. See https://github.com/apache/incubator-heron/issues/1754
@router.get("/executionstate")
async def get_execution_state(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return state.tracker.pb2_to_api(topology)["execution_state"]


@router.get("/schedulerlocation")
async def get_scheduler_location(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return state.tracker.pb2_to_api(topology)["scheduler_location"]


@router.get("/metadata")
async def get_metadata(
    cluster: str, role: str, environ: str, topology: str,
):
  # TODO: deprecate in favour of /info
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  return state.tracker.pb2_to_api(topology)["metadata"]


def topology_stages(logical_plan):
  """Return the number of stages in a logical plan."""
  graph = networkx.DiGraph(
      (input_info["component_name"], bolt_name)
      for bolt_name, bolt_info in logical_plan.get("bolts", {}).items()
      for input_info in bolt_info["inputs"]
  )
  # this is is the same as "diameter" if treating the topology as an undirected graph
  return networkx.dag_longest_path_length(graph)


@router.get("/logicalplan")
async def get_logical_plan(
    cluster: str, role: str, environ: str, topology: str,
):
  """
  This returns a transformed version of the logical plan, it probably
  shouldn't, especially with the renaming. The types should be fixed
  upstream, and the number of topology stages could find somewhere else
  to live.

  """
  topology = state.tracker.get_topology(cluster, role, environ, topology)
  topology_info = state.tracker.pb2_to_api(topology)
  logical_plan = topology_info["logical_plan"]

  # format the logical plan as required by the web (because of Ambrose)
  # first, spouts followed by bolts
  spouts_map = {}
  # XXX: on updates of tracker data, it should be atomic (using copies)
  # TODO: work out types of these and make response model
  for name, value in logical_plan["spouts"].items():
    spouts_map[name] = {
        "config": value.get("config", {}),
        "outputs": value["outputs"],
        "spout_type": value["type"],
        "spout_source": value["source"],
        "extra_links": value["extra_links"],
    }

  bolts_map = {}
  for name, value in logical_plan["bolts"].items():
    bolts_map[name] = {
        "config": value.get("config", {}),
        "inputComponents": [i["component_name"] for i in value["inputs"]],
        "inputs": value["inputs"],
        "outputs": value["outputs"],
    }
  return {
      "stages": topology_stages(logical_plan),
      "spouts": spouts_map,
      "bolts": bolts_map,
  }
