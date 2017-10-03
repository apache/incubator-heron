---
title: Heron's Design Goals
---

From the beginning, Heron was envisioned as a new kind of stream processing
system, built to meet the most demanding of technological requirements, to
handle even the most massive of workloads, and to meet the needs of organizations
of all sizes and degrees of complexity. A few core design goals have guided
Heron's development:

* [Modularity](#modularity)
* [Extensibility](#extensibility)
* [Isolation](#isolation)
* [Constrained resource usage](#constrained-resource-usage)
* [Apache Storm compatibility](#apache-storm-compatibility)

### Modularity

Heron was designed to serve a wide range of requirements, use cases, platforms,
programming languages and so on. In order to suit varying---and often
unforeseeable---needs, Heron 

### Extensibility

Due to its fundamentally [modular](#modularity) character, Heron is remarkably
easy to extend to meet your needs. It offers simple APIs that you can use to
extend Heron to:

* run on not-yet-supported operating systems

### Isolation

Heron [topologies](../topologies) should be process based rather than
thread based, and each process should run in isolation for the sake of easy
debugging, profiling, and troubleshooting.

### Constrained resource usage

Heron [topologies](../topologies) should use only those resources that they are
initially allocated and never exceed those bounds. This makes Heron safe to run
in shared infrastructure.

### Apache Storm compatibility

Although Heron has a [Functional API](../topologies#the-heron-functional-api)
that we recommend for all future topology development, Heron is fully API and
data model compatible with [Apache Storm](http://storm.apache.org), making it
easy for developers to transition from Storm to Heron.

### Backpressure

In a distributed system like Heron, there are no guarantees that all system
components will execute at the same speed. Heron has built-in [back pressure
mechanisms](../architecture#stream-manager) to ensure that topologies can
self-adjust in case components lag.

### Performance

Many of Heron's design choices have enabled Heron to achieve higher throughput
and lower latency than Storm while also offering enhanced configurability to
fine-tune potential latency/throughput trade-offs.

### Semantic guarantees

Heron provides support for
[at-most-once](../delivery-semantics#available-semantics),
[at-least-once](../delivery-semantics#available-semantics), and
[effectively-once](../delivery-semantics#available-semantics) processing
semantics.

### Efficiency

Heron was built with the goal of achieving all of the above with the minumim
possible resource usage.
