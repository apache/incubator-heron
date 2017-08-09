"""
The top-level library for Heron's Python DSL, which enables you to write Heron
[topologies](https://twitter.github.io/heron/docs/concepts/topologies/) in
a Python DSL.

Heron topologies are acyclic graphs used to process streaming data. Topologies
have two major components:
[spouts](spout/spout.m.html#heron_py.spout.spout.Spout) pull data into the
topology and then [emit](spout/spout.m.html#heron_py.spout.spout.Spout.emit)
that data as tuples (lists in Python) to
[bolts](bolt/bolt.m.html#heron_py.bolt.bolt.Bolt) that process that data.
"""

# Load basic dsl modules
from .streamlet import Streamlet, TimeWindow
from .operation import OperationType
