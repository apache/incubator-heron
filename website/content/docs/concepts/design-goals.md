---
title: Heron's Design Goals
---

From the beginning, Heron was envisioned as a new kind of stream processing
system, built to meet the most demanding of technological requirements, to
handle even the most massive of workloads, and to meet the needs of organizations
of all sizes and degrees of complexity. Amongst these requirements:

* The ability to process billions of events per minute
* Extremely low end-to-end latency
* Predictable behavior regardless of scale and in the face of issue like extreme traffic spikes and pipeline congestion
* Simple administration, including:
  * The ability to deploy on shared infrastructure
  * Powerful monitoring capabilities
  * Fine-grained configurability
* Easy debuggability

To meet these requirements, a few core design goals have guided---and continue to
guide---Heron's development:

* [Modularity](#modularity)
* [Extensibility](#extensibility)
* [Isolation](#isolation)
* [Constrained resource usage](#constrained-resource-usage)
* [Apache Storm compatibility](#apache-storm-compatibility)
* [Backpressure handling](#backpressure-handling)
* [Multiple delivery semantics](#multiple-delivery-semantics)

### Modularity

Heron was designed to serve a wide range of requirements, use cases, platforms,
programming languages and so on. In order to suit varying---and often
unforeseeable---needs, Heron provides support for mulitple:

* [schedulers](../architecture#schedulers)
* metrics sinks
* operating systems and platforms
* topology [uploaders](../architecture#schedulers)

### Extensibility

Due to its fundamentally [modular](#modularity) character, Heron is remarkably
easy to extend to meet your needs, with simple APIs that you can use to add
support for new schedulers, programming languages (for topologies), topology
uploaders, etc.

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

### Backpressure handling

In a distributed system like Heron, there are no guarantees that all system
components will execute at the same speed. Heron has built-in [back pressure
mechanisms](../architecture#stream-manager) to ensure that topologies can
self-adjust in case components lag.

### Multiple delivery semantics

Heron provides support for
[at-most-once](../delivery-semantics#available-semantics),
[at-least-once](../delivery-semantics#available-semantics), and
[effectively-once](../delivery-semantics#available-semantics) processing
semantics.
