"""
The top-level library for Heron's Python API, which enables you to build Heron
[topologies](https://twitter.github.io/heron/docs/concepts/topologies/) in
Python.

Heron topologies are acyclic graphs used to process streaming data. Topologies
have two major components:
[spouts](spout/spout.m.html#heron_py.spout.spout.Spout) pull data into the
topology and then [emit](spout/spout.m.html#heron_py.spout.spout.Spout.emit)
that data as tuples (lists in Python) to
[bolts](bolt/bolt.m.html#heron_py.bolt.bolt.Bolt) that process that data.
"""

__all__ = [
    'api_constants',
    'bolt',
    'component',
    'custom_grouping',
    'global_metrics',
    'metrics',
    'serializer',
    'cloudpickle',
    'state',
    'spout',
    'stream',
    'task_hook',
    'topology',
    'topology_context',
    'tuple'
]

# Load basic topology modules
from heron.api.src.python.state.state import State, HashMapState
from heron.api.src.python.state.stateful_component import StatefulComponent
from heron.api.src.python.stream import Stream, Grouping
from heron.api.src.python.topology import Topology, TopologyBuilder
from heron.api.src.python.topology_context import TopologyContext

# Load spout and bolt
from heron.api.src.python.bolt.bolt import Bolt
from heron.api.src.python.bolt.window_bolt import SlidingWindowBolt, TumblingWindowBolt, WindowContext
from heron.api.src.python.spout.spout import Spout
