---
title: Heron Design Goals
---

From its very conception, Heron was built to fulfill a set of core design goals,
including:

### Isolation

[Topologies](/docs/concepts/topologies) should be process based rather than
thread based, and each process should run in isolation for the sake of easy
debugging, profiling, and troubleshooting.

### Resource constraints

Topologies should use only those resources that they are initially allocated and
never exceed those bounds. This makes Heron safe to run in shared infrastructure.

### Compatibility

Heron is fully API and data model compatible with [Apache
Storm](http://storm.apache.org), making it easy for developers to transition
between systems.

### Back pressure

In a distributed system like Heron, there are no guarantees that all system
components will execute at the same speed. Heron has built-in [back pressure
mechanisms]({{< relref "architecture.md#stream-manager" >}}) to ensure that
topologies can self-adjust in case components lag.

### Performance

Many of Heron's design choices have enabled Heron to achieve higher throughput
and lower latency than Storm while also offering enhanced configurability to
fine-tune potential latency/throughput trade-offs.

### Semantic guarantees

Heron provides support for both [at-most-once and
at-least-once](https://kafka.apache.org/08/design.html#semantics) processing
semantics.

### Efficiency

Heron was built with the goal of achieving all of the above with the minumim
possible resource usage.
