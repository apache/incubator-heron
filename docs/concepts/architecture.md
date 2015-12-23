# The Architecture of Heron

Heron is the direct successor of [Apache Storm](http://storm.apache.org). From
an architectural perspective it is markedly different from Storm but fully
backwards compatible with it from an API perspective.

The sections below clarify the distinction between [Heron and
Storm](#releationship-with-apache-storm), describe the [design
goals](#heron-design-goals) behind Heron, and explain major
[components](#components) of its architecture.

## Topologies

You can think of a Heron cluster as a mechanism for managing the lifecycle of
stream-processing entities called **topologies**. More information can be found
in the [Heron Topologies](topologies.html) document.

## Relationship with Apache Storm

Heron is the direct successor of [Apache Storm](http://storm.apache.org) but
built with two goals in mind: 

1. Overcoming Storm's performance, reliability, and other shortcomings by
replacing Storm's thread-based computing model with a process-based model while
also
2. retaining full compatibility with Storm's data model and [topology
API](http://storm.apache.org/tutorial.html).

For a more in-depth discussion of Heron and Storm, see the [Twitter Heron:
Stream Processing at Scale](http://dl.acm.org/citation.cfm?id=2742788) paper.

## Heron Design Goals

* **Isolation** &mdash; [Topologies](topologies.html) should be process based
  rather than thread based, and each process should run in isolation for the
  sake of easy debugging, profiling, and troubleshooting.
* **Resource constraints** &mdash; Topologies should use only those resources
  that they are initially allocated and never exceed those bounds. This makes
  Heron safe to run in shared infrastructure.
* **Compatibility** &mdash; Heron is fully API and data model compatible with
  [Apache Storm](#relationship-with-apache-storm), making it easy for developers
  to transition between systems.
* **Back pressure** &mdash; In a distributed system like Heron, there are no
  guarantees that all system components will execute at the same speed. Heron
  has built-in [back pressure mechanisms](#stream-manager) to ensure that
  topologies can self-adjust in case components lag.
* **Performance** &mdash; Many of Heron's design choices have enabled Heron to
  achieve higher throughput and lower latency than Storm while also offering
  enhanced configurability to fine-tune potential latency/throughput trade-offs.
* **Semantic guarantees** &mdash; Heron provides support for both
  [at-most-once and at-least-once](https://kafka.apache.org/08/design.html#semantics)
  processing semantics.
* **Efficiency** &mdash; Heron was built with the goal of achieving all of the
  above with the minimal possible resource usage.

## Components

The following core components of Heron will be discussed in depth in the
sections below:

* [Topology Master](#topology-master)
* [Container](#container)
* [Stream Manager](#stream-manager)
* [Heron Instance](#heron-instance)
* [Metrics Manager](#metrics-manager)
* [Heron Tracker](#heron-tracker)

### Topology Master

The Topology Master (TM) manages a topology throughout its entire lifecycle,
from the time it's submitted until it's finally killed. When `heron-cli` starts
up a new TM, the TM creates an ephemeral
[ZooKeeper](http://zookeeper.apache.org) node that ensures that there's only one
TM for the topology and that the TM is easily discoverable for any process in
the topology. The TM is also responsible for constructing a [physical
plan](topologies.html#physica-plan) for a topology which is then relayed to
different components.

![Topology Master](img/tmaster.png)

#### Topology Master Configuration

TMs have a variety of [configurable
parameters](../operators/configuration/tmaster.html) that you can adjust at each
phase of a topology's [lifecycle](topologies.html#topology-lifecycle).

### Container

Each Heron topology consists of multiple **containers**, each of which houses
multiple [Heron Instances](#heron-instance), a [Stream
Manager](#stream-manager), and a [Metrics Manager](#metrics-manager). Containers
communicate with the topology's TM to ensure that the topology forms a fully
connected graph.

For an illustration, see the figure in the [Topology Master](#topology-master)
section above.

### Stream Manager

The **Stream Manager** (SM) manages the routing of tuples between topology
components. Each [Heron Instance](#heron-instance) in a topology connects to its
local SM, while all of the SMs in a given topology connect to one another to
form a network. Here's a visual illustration of a network of SMs:

![Heron Data Flow](img/data-flow.png)

In addition to being a routing engine for data streams, SMs are responsible for
propagating [back pressure](https://en.wikipedia.org/wiki/Back_pressure)
throughout topologies when necessary. Below is an illustration of back pressure:

![Back Pressure 1](img/backpressure1.png)

In the diagram above, assume that bolt **B3** (in container **A**) receives all
of its inputs from spout **S1**. **B3** is running more slowly than other
components. In response, the SM for container **A** will refuse input from the
SMs in containers **C** and **D**, which will lead to the socket buffers in
those containers filling up, which could lead to throughput collapse.

In a situation like this, Heron's back pressure mechanism will kick in. The SM
in container **A** will send a message to all the other SMs. In response, the
other SMs will examine the container's [physical
plan](topologies.html#physical-plan) and cut off inputs from spouts that feed
bolt **B3** (in this case spout **S1**).

![Back Pressure 2](img/backpressure2.png)

Once the lagging bolt (**B3**) begins functioning normally, the SM in container
**A** will notify the other SMs and stream routing within the topology will
return to normal.

#### Stream Manger Configuration

SMs have a variety of [configurable
parameters](../operators/configuration/stmgr.html) that you can adjust at each
phase of a topology's [lifecycle](topologies.html#topology-lifecycle).

### Heron Instance

A **Heron Instance** (HI) is a process that handles a single task of a
[spout](topologies.html#spouts) or [bolt](topologies.html#bolts), which allows
for easy debugging and profiling.

Currently, Heron only supports Java, so all
HIs are [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine) processes, but
this will change in the future.

#### Heron Instance Configuration

HIs have a variety of [configurable
parameters](../operators/configuration/instance.html) that you can adjust at
each phase of a topology's [lifecycle](topologies.html#topology-lifecycle).

### Metrics Manager

Each topology runs a Metrics Manager (MM) that collects and exports metrics from
all components in a [container](#container). It then routes those metrics to
both the [TM](#topology-master) and to external collectors, such as
[Scribe](https://github.com/facebookarchive/scribe),
[Graphite](http://graphite.wikidot.com/), or analogous systems.

You can adapt Heron to support additional systems by implementing your own
[metrics sink](../contributors/metrics-sink.html).

### Heron Tracker

The **Heron Tracker** (or just Tracker) is a centralized gateway for
cluster-wide information about topologies, including which topologies are
running, being launched, being killed, etc. It relies on the same
[ZooKeeper](http://zookeeper.apache.org) nodes as the topologies in the cluster
and then exposes that information through a JSON REST API. The Tracker can be
run inside of your Heron cluster (on the same set of machines managed by your
Heron [scheduler](../operators/deployment/index.html)) or outside of it.

Instructions on running the tracker can be found in [Heron
Tracker](../operators/heron-tracker.html). API docs for the Tracker's JSON API
can be found in [The Heron Tracker REST
API](../operators/heron-tracker/heron-tracker-rest-api.html)

## Heron UI

**Heron UI** is a rich visual interface that you can use to interact with
topologies. Through Heron UI you can see color-coded visual representations of
the [logical](topologies.html#logical-plan) and
[physical](topologies.html#physical-plan) plan of each topology in your cluster.

For more information, see the [Heron UI](../operators/heron-ui.html) document.
