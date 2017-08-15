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
import heron.api.src.python.api_constants as api_constants
import heron.api.src.python.global_metrics as global_metrics
from heron.api.src.python.custom_grouping import ICustomGrouping
from heron.api.src.python.metrics import IMetric, CountMetric, MultiCountMetric
from heron.api.src.python.metrics import IReducer, MeanReducer, ReducedMetric, MultiReducedMetric
from heron.api.src.python.metrics import AssignableMetrics, MultiAssignableMetrics
from heron.api.src.python.metrics import MeanReducedMetric, MultiMeanReducedMetric
from heron.api.src.python.serializer import IHeronSerializer, PythonSerializer, default_serializer
from heron.api.src.python.task_hook import ITaskHook
from heron.api.src.python.tuple import Tuple, TupleHelper

# Load spout and bolt
from heron.api.src.python.bolt.bolt import Bolt
from heron.api.src.python.bolt.window_bolt import SlidingWindowBolt, TumblingWindowBolt, WindowContext
from heron.api.src.python.spout.spout import Spout
