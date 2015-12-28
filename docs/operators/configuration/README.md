# Heron Configuration

Heron can be configured at two levels:

1. **The system level** &mdash; System-level configurations apply to the whole
Heron cluster rather than to any specific topology.
2. **The topology level** &mdash; Topology configurations apply only to a
specific topology and can be modified at any stage of the topology's
[lifecycle](../../concepts/topologies.html#topology-lifecycle).

## The System Level

There are a small handful of system-level configs for Heron. These are detailed
in [System-level Configuration](system.html).

## The Topology Level

### Default vs. User-defined Configuration

You can find lists of configurable parameters on a topology-wide or
per-component basis in the following docs:

* [System-level Configuration](system.html)
* [Heron Instance](instance.html)
* [Heron Metrics Manager](metrics-manager.html)
* [Heron Topology Master](topology-master.html)
* [Heron Stream Manager](stream-manager.html)

You can override any
