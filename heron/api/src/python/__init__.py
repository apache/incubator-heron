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
from .state import State, StatefulComponent, HashMapState
from .stream import Stream, Grouping
from .topology import Topology, TopologyBuilder
from .topology_context import TopologyContext

# Load spout and bolt
from .bolt import Bolt, SlidingWindowBolt, TumblingWindowBolt, WindowContext
from .spout import Spout
